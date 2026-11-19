const pathCache = new Map();

function getBotTokens(env) {
  return (env.BOT_TOKENS || env.BOT_TOKEN || "").split(",").map(s => s.trim()).filter(Boolean);
}

async function getTgFileUrl(fileId, botToken) {
  const key = `${botToken.slice(-8)}:${fileId}`;
  const now = Date.now();
  const cached = pathCache.get(key);
  if (cached && cached.exp > now) return cached.url;

  const res = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(fileId)}`);
  const data = await res.json();
  if (!data.ok) throw new Error("getFile error");

  const dlUrl = `https://api.telegram.org/file/bot${botToken}/${data.result.file_path}`;
  pathCache.set(key, { url: dlUrl, exp: now + 50 * 60 * 1000 });
  return dlUrl;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, POST, HEAD, OPTIONS" };
    if (request.method === "OPTIONS") return new Response(null, { headers: cors });

    const tokens = getBotTokens(env);

    // 1. Streaming Chunk
    if (url.pathname === "/c") {
      const id = url.searchParams.get("id");
      const b = parseInt(url.searchParams.get("b") || "0", 10);
      const token = tokens[b] || tokens[0];
      if (!id || !token) return new Response("Bad request", { status: 400 });

      const tgUrl = await getTgFileUrl(id, token);
      const tgRes = await fetch(tgUrl);
      if (!tgRes.ok) return new Response("Upstream error", { status: 502 });

      return new Response(tgRes.body, {
        status: 200,
        headers: { ...cors, "Content-Type": "video/mp2t", "Cache-Control": "public, max-age=86400" },
      });
    }

    // 2. Full MP4 Download (POST from player.html OR GET from website)
    if (url.pathname === "/dl") {
      let meta, filename = url.searchParams.get("name") || "video.mp4";
      if (request.method === "POST") {
        const fd = await request.formData();
        meta = JSON.parse(fd.get("payload"));
        if (meta.filename) filename = meta.filename;
      } else {
        const manifestUrl = url.searchParams.get("manifest");
        if (!manifestUrl) return new Response("Missing manifest", { status: 400 });
        meta = await (await fetch(manifestUrl)).json();
        if (meta.filename && !url.searchParams.get("name")) filename = meta.filename;
      }

      const safeName = filename.replace(/["\\/\r\n]/g, "_");
      const total = meta.total_bytes;

      let start = 0, end = total - 1, isRange = false;
      const rangeHdr = request.headers.get("Range");
      if (request.method === "GET" && rangeHdr && rangeHdr.startsWith("bytes=")) {
        const [sStr, eStr] = rangeHdr.slice(6).split("-");
        if (sStr === "" && eStr) start = Math.max(0, total - parseInt(eStr, 10));
        else {
          start = parseInt(sStr || "0", 10);
          if (eStr) end = Math.min(total - 1, parseInt(eStr, 10));
        }
        isRange = true;
      }

      if (start >= total || start > end) {
        return new Response(null, { status: 416, headers: { ...cors, "Content-Range": `bytes */${total}` } });
      }

      const contentLen = end - start + 1;
      const baseHeaders = {
        ...cors,
        "Content-Type": "video/mp4",
        "Accept-Ranges": "bytes",
        "ETag": `"${total}"`,
        "Content-Length": String(contentLen),
        "Content-Disposition": `attachment; filename="${safeName}"`,
        ...(isRange ? { "Content-Range": `bytes ${start}-${end}/${total}` } : {}),
      };

      if (request.method === "HEAD") {
        return new Response(null, { status: isRange ? 206 : 200, headers: baseHeaders });
      }

      const { readable, writable } = new TransformStream();
      (async () => {
        const writer = writable.getWriter();
        try {
          let partOffset = 0;
          for (const part of meta.parts) {
            const pStart = partOffset, pEnd = partOffset + part.size - 1;
            partOffset += part.size;
            if (pEnd < start) continue;
            if (pStart > end) break;

            const skipStart = Math.max(0, start - pStart);
            const keepEnd = Math.min(part.size - 1, end - pStart);
            const needBytes = keepEnd - skipStart + 1;

            const tgUrl = await getTgFileUrl(part.id, tokens[part.b || 0] || tokens[0]);
            const r = await fetch(tgUrl);
            const reader = r.body.getReader();

            let seen = 0, emitted = 0;
            while (true) {
              const { done, value } = await reader.read();
              if (done) break;
              const cStart = seen, cEnd = seen + value.byteLength - 1;
              seen += value.byteLength;
              if (cEnd < skipStart) continue;
              const sliceFrom = Math.max(0, skipStart - cStart);
              const sliceTo = Math.min(value.byteLength, sliceFrom + (needBytes - emitted));
              const sub = (sliceFrom === 0 && sliceTo === value.byteLength) ? value : value.subarray(sliceFrom, sliceTo);
              await writer.write(sub);
              emitted += sub.byteLength;
              if (emitted >= needBytes) { await reader.cancel(); break; }
            }
          }
          await writer.close();
        } catch (err) {
          await writer.abort(err);
        }
      })();

      return new Response(readable, { status: isRange ? 206 : 200, headers: baseHeaders });
    }

    return new Response("ABR CDN Ready!");
  },
};
