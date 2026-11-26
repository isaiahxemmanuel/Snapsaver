'use strict';
/* Snapsaver backend. Zero dependencies, Node 18+.
   Nothing is saved: media streams from Snapchat's CDN straight to the user. */
const http = require('http');
const fs = require('fs');
const path = require('path');
const { Readable, pipeline } = require('stream');

const PORT = process.env.PORT || 3000;
const UA = 'Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36';
const PAGE_HOSTS = ['snapchat.com'];
const MEDIA_HOSTS = ['sc-cdn.net', 'snapchat.com']
  .concat((process.env.EXTRA_MEDIA_HOSTS || '').split(',').map(s => s.trim()).filter(Boolean));

// Works with the page in public/index.html or sitting next to server.js (easier to upload from a phone)
const INDEX = [path.join(__dirname, 'public', 'index.html'), path.join(__dirname, 'index.html')].find(f => fs.existsSync(f));
const PAGES = {};
['privacy', 'terms', 'about', 'contact'].forEach(n => {
  PAGES['/' + n] = [path.join(__dirname, 'public', n + '.html'), path.join(__dirname, n + '.html')].find(f => fs.existsSync(f));
});

const hostOk = (h, list) => { h = String(h).toLowerCase(); return list.some(d => h === d || h.endsWith('.' + d)); };
const safeUrl = (s, list) => {
  try { const u = new URL(s); return u.protocol === 'https:' && hostOk(u.hostname, list) ? u : null; }
  catch (e) { return null; }
};
const decode = s => String(s).replace(/\\u002[Ff]/g, '/').replace(/\\\//g, '/').replace(/&amp;/g, '&');

/* ---------- finding the media inside a public page ---------- */
const BAD_KEY = /thumb|preview|bitmoji|avatar|icon|poster|logo|profile|sticker|lens|overlay/i;
const URL_KEY = /(media|video|content|stream|download)_?url$/i;

function walk(node, fn, key) {
  if (Array.isArray(node)) node.forEach(v => walk(v, fn, key));
  else if (node && typeof node === 'object') {
    fn(node, key || '');
    Object.keys(node).forEach(k => walk(node[k], fn, k));
  }
}

function extractMedia(html) {
  const out = [], seen = new Set(), rejected = new Set();
  const add = (raw, type) => {
    const u = safeUrl(decode(raw), MEDIA_HOSTS);
    if (!u) { try { rejected.add(new URL(decode(raw)).hostname); } catch (e) {} return; }
    if (seen.has(u.href)) return;
    seen.add(u.href); out.push({ url: u.href, type });
  };

  const blobs = [];
  for (const m of html.matchAll(/<script[^>]*type=["']application\/(?:ld\+)?json["'][^>]*>([\s\S]*?)<\/script>/gi)) {
    try { blobs.push(JSON.parse(m[1])); } catch (e) {}
  }

  // 1. Public stories: a list of snaps, each with a media address
  blobs.forEach(b => walk(b, o => {
    if (o.snapUrls && typeof o.snapUrls.mediaUrl === 'string') {
      const video = o.snapMediaType === 1 || /video/i.test(String(o.snapMediaType)) || /\.mp4/i.test(o.snapUrls.mediaUrl);
      add(o.snapUrls.mediaUrl, video ? 'video' : 'image');
    }
  }));
  if (out.length) return { items: out, rejected: [...rejected] };

  // 2. Spotlight and shared videos: social-preview tags and schema.org data
  for (const m of html.matchAll(/<meta\s[^>]*>/gi)) {
    const k = /(?:property|name)=["']([^"']+)["']/i.exec(m[0]);
    const c = /content=["']([^"']*)["']/i.exec(m[0]);
    if (k && c && /^(og:video(:secure_url|:url)?|twitter:player:stream)$/i.test(k[1])) add(c[1], 'video');
  }
  blobs.forEach(b => walk(b, o => { if (typeof o.contentUrl === 'string') add(o.contentUrl, 'video'); }));
  if (out.length) return { items: out, rejected: [...rejected] };

  // 3. Any media-looking address in the page data
  blobs.forEach(b => walk(b, (o, key) => {
    if (BAD_KEY.test(key)) return;
    Object.keys(o).forEach(k => {
      if (typeof o[k] === 'string' && URL_KEY.test(k) && !BAD_KEY.test(k)) {
        const v = o[k];
        const isVideo = /\.(mp4|mov|m4v|webm)(\?|$)/i.test(v) || /video|stream/i.test(k);
        add(v, isVideo ? 'video' : 'image');
      }
    });
  }));
  if (out.length) return { items: out, rejected: [...rejected] };

  // 4. Last resort: any .mp4 address in the raw page
  for (const m of decode(html).matchAll(/https:\/\/[a-z0-9.-]+\/[^"'\s<>\\]*?\.mp4[^"'\s<>\\]*/gi)) add(m[0], 'video');
  return { items: out, rejected: [...rejected] };
}

const kindOf = u => {
  const p = u.pathname.toLowerCase();
  if (p.includes('/spotlight/')) return 'Spotlight video';
  if (p.includes('/add/') || u.hostname.startsWith('story.') || p.includes('/s/')) return 'Public story';
  return 'Snapchat video';
};

/* ---------- network helpers ---------- */
async function fetchFollow(start, list, headers, hops) {
  let u = start;
  for (let i = 0; i <= hops; i++) {
    const ac = new AbortController(), t = setTimeout(() => ac.abort(), 12000);
    let r;
    try {
      r = await fetch(u, { redirect: 'manual', signal: ac.signal,
        headers: Object.assign({ 'User-Agent': UA, 'Accept-Language': 'en-US,en;q=0.9' }, headers) });
    } finally { clearTimeout(t); }
    const loc = r.headers.get('location');
    if (r.status >= 300 && r.status < 400 && loc) {
      if (r.body) r.body.cancel().catch(() => {});
      const next = safeUrl(new URL(loc, u).href, list);
      if (!next) throw new Error('bad_redirect');
      u = next.href;
      continue;
    }
    return r;
  }
  throw new Error('too_many_redirects');
}

async function readLimited(res, max) {
  const chunks = []; let n = 0;
  for await (const c of res.body) {
    n += c.length;
    if (n > max) throw new Error('too_large');
    chunks.push(c);
  }
  return Buffer.concat(chunks).toString('utf8');
}

/* ---------- small helpers ---------- */
const hits = new Map();
function limited(ip, bucket, max) {
  const k = bucket + ip, now = Date.now();
  let h = hits.get(k);
  if (!h || h.reset < now) { h = { n: 0, reset: now + 60000 }; hits.set(k, h); }
  return ++h.n > max;
}
setInterval(() => { const now = Date.now(); for (const [k, h] of hits) if (h.reset < now) hits.delete(k); }, 60000).unref();
const ipOf = req => String((req.headers['x-forwarded-for'] || '').split(',')[0] || req.socket.remoteAddress || '').trim();

function send(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(body);
}
function readBody(req, max) {
  return new Promise((ok, no) => {
    let s = '';
    req.on('data', c => { s += c; if (s.length > max) { no(new Error('too_large')); req.destroy(); } });
    req.on('end', () => ok(s));
    req.on('error', no);
  });
}
const safeName = n => String(n || 'snapsaver').replace(/[^a-z0-9._-]/gi, '-').slice(0, 80);

/* ---------- routes ---------- */
async function resolve(req, res) {
  if (limited(ipOf(req), 'r', 15)) return send(res, 429, { ok: false, error: 'rate_limited' });
  let link = '';
  try { link = String(JSON.parse(await readBody(req, 4096)).url || '').trim(); }
  catch (e) { return send(res, 400, { ok: false, error: 'bad_request' }); }
  if (link && !/^https?:\/\//i.test(link)) link = 'https://' + link;
  const page = safeUrl(link, PAGE_HOSTS);
  if (!page) return send(res, 400, { ok: false, error: 'not_snapchat' });

  let html, finalUrl = page;
  try {
    const r = await fetchFollow(page.href, PAGE_HOSTS, { Accept: 'text/html,application/xhtml+xml' }, 4);
    if (!r.ok) return send(res, 502, { ok: false, error: 'fetch_failed' });
    html = await readLimited(r, 4e6);
  } catch (e) { return send(res, 502, { ok: false, error: 'fetch_failed' }); }

  const { items, rejected } = extractMedia(html);
  console.log('resolve items=%d rejectedHosts=%s', items.length, rejected.join(',') || '-');
  if (!items.length) return send(res, 404, { ok: false, error: 'no_media' });
  send(res, 200, {
    ok: true,
    kind: kindOf(finalUrl),
    items: items.slice(0, 30).map((it, i) => {
      const name = 'snapsaver-' + (i + 1) + (it.type === 'video' ? '.mp4' : '.jpg');
      const q = '/api/download?u=' + encodeURIComponent(it.url) + '&name=' + name;
      return { type: it.type, name, src: q + '&inline=1', href: q };
    })
  });
}

async function download(req, res) {
  if (limited(ipOf(req), 'd', 90)) return send(res, 429, { ok: false, error: 'rate_limited' });
  const q = new URL(req.url, 'http://x').searchParams;
  const media = safeUrl(q.get('u') || '', MEDIA_HOSTS);
  if (!media) return send(res, 400, { ok: false, error: 'bad_request' });
  const headers = req.headers.range ? { Range: req.headers.range } : {};
  let up;
  try { up = await fetchFollow(media.href, MEDIA_HOSTS, headers, 3); }
  catch (e) { return send(res, 502, { ok: false, error: 'fetch_failed' }); }
  const type = up.headers.get('content-type') || '';
  if (![200, 206].includes(up.status) || !up.body || !/^(video|image)\/|^application\/octet-stream/i.test(type)) {
    if (up.body) up.body.cancel().catch(() => {});
    return send(res, 502, { ok: false, error: 'fetch_failed' });
  }
  const h = { 'Content-Type': type, 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' };
  ['content-length', 'content-range', 'accept-ranges'].forEach(k => { const v = up.headers.get(k); if (v) h[k] = v; });
  if (!q.get('inline')) h['Content-Disposition'] = 'attachment; filename="' + safeName(q.get('name')) + '"';
  res.writeHead(up.status, h);
  pipeline(Readable.fromWeb(up.body), res, () => {});
}

const server = http.createServer(async (req, res) => {
  res.setHeader('Referrer-Policy', 'no-referrer');
  const p = new URL(req.url, 'http://x').pathname;
  try {
    if (req.method === 'GET' && (p === '/' || p === '/index.html')) {
      if (!INDEX) return send(res, 500, { ok: false, error: 'server_error' });
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      fs.createReadStream(INDEX).pipe(res);
    } else if (req.method === 'GET' && PAGES[p.replace(/\/$/, '')]) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      fs.createReadStream(PAGES[p.replace(/\/$/, '')]).pipe(res);
    } else if (p === '/healthz') send(res, 200, { ok: true });
    else if (p === '/api/resolve' && req.method === 'POST') await resolve(req, res);
    else if (p === '/api/download' && req.method === 'GET') await download(req, res);
    else send(res, 404, { ok: false, error: 'not_found' });
  } catch (e) {
    if (!res.headersSent) send(res, 500, { ok: false, error: 'server_error' }); else res.destroy();
  }
});

module.exports = { extractMedia, hostOk, safeUrl, server };
if (require.main === module) server.listen(PORT, () => console.log('Snapsaver listening on ' + PORT));
