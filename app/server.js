// Tidefin — Jellyfin music player with Tidal search + Tidarr downloads.
// No npm dependencies: runs on the stock node:22-alpine image.
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const { Readable } = require('stream');

const env = (k, d = '') => (process.env[k] || d).trim();
const PORT = parseInt(env('PORT', '8585'), 10);
const JF_URL = env('JELLYFIN_URL').replace(/\/+$/, '');
const JF_KEY = env('JELLYFIN_API_KEY');
const TD_URL = env('TIDARR_URL').replace(/\/+$/, '');
const TD_KEY = env('TIDARR_API_KEY');
const TIDAL_ID = env('TIDAL_CLIENT_ID');
const TIDAL_SECRET = env('TIDAL_CLIENT_SECRET');
const COUNTRY = env('TIDAL_COUNTRY', 'NO').toUpperCase();
const APP_PASSWORD = env('APP_PASSWORD');
const PUBLIC_DIR = path.join(__dirname, 'public');

// ---------- helpers ----------
function send(res, code, body, type = 'application/json') {
  const data = type === 'application/json' ? JSON.stringify(body) : body;
  res.writeHead(code, { 'Content-Type': type, 'Cache-Control': 'no-store' });
  res.end(data);
}
const fail = (res, code, msg) => send(res, code, { error: msg });
const why = (e) => e.message + (e.cause ? ` (${e.cause.code || e.cause.message}${e.cause.address ? ' ' + e.cause.address + ':' + e.cause.port : ''})` : '');
// wrap fetch so network errors say which service failed
const rawFetch = global.fetch;
const fetch = async (url, opts) => {
  try { return await rawFetch(url, opts); }
  catch (e) {
    const host = (() => { try { return new URL(url).origin; } catch { return String(url); } })();
    const err = new Error(`Cannot reach ${host}`); err.cause = e.cause || e; throw err;
  }
};

async function readBody(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  if (!chunks.length) return {};
  try { return JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch { return {}; }
}

// Pipe an upstream fetch Response to the client, keeping Range/streaming headers.
function pipeUpstream(up, res) {
  const headers = {};
  for (const h of ['content-type', 'content-length', 'content-range', 'accept-ranges', 'cache-control', 'last-modified', 'etag']) {
    const v = up.headers.get(h);
    if (v) headers[h] = v;
  }
  res.writeHead(up.status, headers);
  if (!up.body) return res.end();
  const s = Readable.fromWeb(up.body);
  s.on('error', () => res.destroy());
  res.on('close', () => s.destroy());
  s.pipe(res);
}

// ---------- Jellyfin ----------
const jfHeaders = () => ({
  Authorization: `MediaBrowser Client="Tidefin", Device="NAS", DeviceId="tidefin", Version="1.0.0", Token="${JF_KEY}"`,
  Accept: 'application/json',
});

async function jf(pathAndQuery, opts = {}) {
  if (!JF_URL || !JF_KEY) throw new Error('Jellyfin is not configured (JELLYFIN_URL / JELLYFIN_API_KEY)');
  const r = await fetch(JF_URL + pathAndQuery, { ...opts, headers: { ...jfHeaders(), ...(opts.headers || {}) } });
  if (!r.ok) throw new Error(`Jellyfin ${r.status} on ${pathAndQuery.split('?')[0]}`);
  const t = await r.text();
  return t ? JSON.parse(t) : {};
}

const jfFields = 'Fields=PrimaryImageAspectRatio,ProductionYear,ChildCount,DateCreated';
const mapJfAlbum = (i) => ({
  id: i.Id, title: i.Name, artist: i.AlbumArtist || (i.AlbumArtists?.[0]?.Name) || (i.Artists || []).join(', '),
  year: i.ProductionYear || null, hasImage: !!(i.ImageTags && i.ImageTags.Primary),
});
const mapJfTrack = (i) => ({
  id: i.Id, title: i.Name, artist: (i.Artists || []).join(', ') || i.AlbumArtist || '',
  album: i.Album || '', albumId: i.AlbumId || null, number: i.IndexNumber || null, disc: i.ParentIndexNumber || null,
  duration: i.RunTimeTicks ? Math.round(i.RunTimeTicks / 1e7) : null,
  imageId: (i.ImageTags && i.ImageTags.Primary) ? i.Id : (i.AlbumPrimaryImageTag ? i.AlbumId : null),
});

// ---------- Tidal official API (search + metadata only) ----------
let tidalToken = null, tidalTokenExp = 0;
async function tidalAuth() {
  if (tidalToken && Date.now() < tidalTokenExp - 60000) return tidalToken;
  if (!TIDAL_ID || !TIDAL_SECRET) throw new Error('Tidal is not configured (TIDAL_CLIENT_ID / TIDAL_CLIENT_SECRET)');
  const r = await fetch('https://auth.tidal.com/v1/oauth2/token', {
    method: 'POST',
    headers: {
      Authorization: 'Basic ' + Buffer.from(`${TIDAL_ID}:${TIDAL_SECRET}`).toString('base64'),
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: 'grant_type=client_credentials',
  });
  if (!r.ok) throw new Error(`Tidal login failed (${r.status}) — check client ID/secret`);
  const j = await r.json();
  tidalToken = j.access_token;
  tidalTokenExp = Date.now() + (j.expires_in || 3600) * 1000;
  return tidalToken;
}

async function tidal(p, params = []) {
  const qs = new URLSearchParams([['countryCode', COUNTRY], ...params]).toString();
  const doFetch = async () => fetch(`https://openapi.tidal.com/v2${p}?${qs}`, {
    headers: { Authorization: `Bearer ${await tidalAuth()}`, Accept: 'application/vnd.api+json' },
  });
  let r = await doFetch();
  if (r.status === 401) { tidalToken = null; r = await doFetch(); }
  if (!r.ok) {
    const body = await r.text().catch(() => '');
    let detail = '';
    try { const j = JSON.parse(body); detail = (j.errors || []).map((x) => x.detail || x.code).join('; '); } catch { detail = body.slice(0, 200); }
    const e = new Error(`Tidal ${r.status}${detail ? ': ' + detail : ''} (${p})`); e.status = r.status; throw e;
  }
  return r.json();
}

// ISO-8601 duration (PT3M25S) -> seconds
function isoDur(s) {
  const m = /^P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:([\d.]+)S)?$/.exec(s || '');
  if (!m) return null;
  return Math.round((+m[1] || 0) * 86400 + (+m[2] || 0) * 3600 + (+m[3] || 0) * 60 + (+m[4] || 0));
}

function indexIncluded(doc) {
  const map = new Map();
  for (const r of doc.included || []) map.set(`${r.type}:${r.id}`, r);
  for (const r of Array.isArray(doc.data) ? doc.data : []) map.set(`${r.type}:${r.id}`, r);
  return map;
}
const relIds = (res, rel) => {
  const d = res?.relationships?.[rel]?.data;
  return Array.isArray(d) ? d : d ? [d] : [];
};
function pickCover(map, album, size = 320) {
  for (const ref of relIds(album, 'coverArt')) {
    const art = map.get(`artworks:${ref.id}`);
    const files = art?.attributes?.files || [];
    if (!files.length) continue;
    const sorted = [...files].sort((a, b) => (a.meta?.width || 0) - (b.meta?.width || 0));
    return (sorted.find((f) => (f.meta?.width || 0) >= size) || sorted[sorted.length - 1]).href;
  }
  return null;
}
const artistNames = (map, res) => relIds(res, 'artists').map((a) => map.get(`artists:${a.id}`)?.attributes?.name).filter(Boolean).join(', ');

// Fetch resources by id with relationships; falls back if nested include is rejected.
async function tidalByIds(type, ids, includes, fallbackIncludes) {
  if (!ids.length) return { data: [], included: [] };
  const params = (inc) => [...ids.map((id) => ['filter[id]', id]), ...inc.map((i) => ['include', i])];
  try { return await tidal(`/${type}`, params(includes)); }
  catch (e) { if (e.status === 400 && fallbackIncludes) return tidal(`/${type}`, params(fallbackIncludes)); throw e; }
}

async function tidalSearch(q) {
  // GET /searchResults?filter[query]=… returns one searchResults resource whose
  // relationships hold the ordered track/album ids.
  const sDoc = await tidal('/searchResults', [
    ['filter[query]', q], ['explicitFilter', 'INCLUDE'], ['include', 'tracks'], ['include', 'albums'],
  ]);
  const sRes = Array.isArray(sDoc.data) ? sDoc.data[0] : sDoc.data;
  const trackIds = relIds(sRes, 'tracks').filter((d) => d.type === 'tracks').map((d) => d.id).slice(0, 20);
  const albumIds = relIds(sRes, 'albums').filter((d) => d.type === 'albums').map((d) => d.id).slice(0, 12);
  const [tDoc, aDoc] = await Promise.all([
    tidalByIds('tracks', trackIds, ['artists', 'albums', 'albums.coverArt'], ['artists', 'albums']),
    tidalByIds('albums', albumIds, ['artists', 'coverArt'], ['artists']),
  ]);
  const tMap = indexIncluded(tDoc), aMap = indexIncluded(aDoc);
  const order = (ids, list) => ids.map((id) => list.find((x) => String(x.id) === String(id))).filter(Boolean);
  const tracks = order(trackIds, tDoc.data || []).map((t) => {
    const alb = tMap.get(`albums:${relIds(t, 'albums')[0]?.id}`);
    return {
      id: t.id, title: t.attributes?.title + (t.attributes?.version ? ` (${t.attributes.version})` : ''),
      artist: artistNames(tMap, t), album: alb?.attributes?.title || '', albumId: alb?.id || null,
      duration: isoDur(t.attributes?.duration), explicit: !!t.attributes?.explicit,
      cover: alb ? pickCover(tMap, alb) : null,
    };
  });
  const albums = order(albumIds, aDoc.data || []).map((a) => ({
    id: a.id, title: a.attributes?.title, artist: artistNames(aMap, a),
    year: (a.attributes?.releaseDate || '').slice(0, 4) || null, cover: pickCover(aMap, a),
    tracks: a.attributes?.numberOfItems || null,
  }));
  return { tracks, albums };
}

async function tidalAlbum(id) {
  const aDoc = await tidalByIds('albums', [id], ['artists', 'coverArt'], ['artists']);
  const aMap = indexIncluded(aDoc);
  const a = (aDoc.data || [])[0];
  // album items are paginated: follow page[cursor] until done
  const refs = [];
  let cursor = null;
  for (let page = 0; page < 15; page++) {
    const doc = await tidal(`/albums/${encodeURIComponent(id)}/relationships/items`, cursor ? [['page[cursor]', cursor]] : []);
    refs.push(...(doc.data || []));
    const next = doc.links?.next;
    cursor = next ? new URL(next, 'https://openapi.tidal.com').searchParams.get('page[cursor]') : null;
    if (!cursor) break;
  }
  const ids = refs.filter((d) => d.type === 'tracks').map((d) => ({ id: d.id, meta: d.meta || {} }));
  // fetch track artists in chunks of 20
  const tracks = [];
  for (let i = 0; i < ids.length; i += 20) {
    const chunk = ids.slice(i, i + 20);
    const doc = await tidalByIds('tracks', chunk.map((c) => c.id), ['artists']);
    const m = indexIncluded(doc);
    for (const c of chunk) {
      const t = (doc.data || []).find((x) => String(x.id) === String(c.id));
      if (!t) continue;
      tracks.push({
        id: t.id, title: t.attributes?.title + (t.attributes?.version ? ` (${t.attributes.version})` : ''),
        artist: artistNames(m, t), number: c.meta.trackNumber || null, disc: c.meta.volumeNumber || null,
        duration: isoDur(t.attributes?.duration), explicit: !!t.attributes?.explicit,
      });
    }
  }
  return {
    id, title: a?.attributes?.title || '', artist: a ? artistNames(aMap, a) : '',
    year: (a?.attributes?.releaseDate || '').slice(0, 4) || null, cover: a ? pickCover(aMap, a, 640) : null, tracks,
  };
}

// ---------- Tidarr ----------
async function tidarr(p, opts = {}) {
  if (!TD_URL || !TD_KEY) throw new Error('Tidarr is not configured (TIDARR_URL / TIDARR_API_KEY)');
  return fetch(TD_URL + p, {
    ...opts,
    headers: { 'X-Api-Key': TD_KEY, 'Content-Type': 'application/json', Accept: 'application/json', ...(opts.headers || {}) },
  });
}

async function tidarrQueue() {
  const r = await tidarr('/api/queue/list?offset=0&limit=200');
  if (!r.ok) throw new Error(`Tidarr ${r.status}`);
  const j = await r.json();
  const list = Array.isArray(j) ? j : (j.items || j.data || j.queue || j.list || []);
  return list.map((i) => ({
    id: String(i.id), type: i.type, status: i.status, title: i.title || i.name || '', artist: i.artist || '',
    progress: i.progress ?? null, error: i.error || null,
  }));
}

// ---------- auth (optional) ----------
function authorized(req) {
  if (!APP_PASSWORD) return true;
  const h = req.headers.authorization || '';
  if (!h.startsWith('Basic ')) return false;
  const pass = Buffer.from(h.slice(6), 'base64').toString().split(':').slice(1).join(':');
  return pass === APP_PASSWORD;
}

// ---------- router ----------
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.png': 'image/png', '.json': 'application/json', '.webmanifest': 'application/manifest+json' };

const server = http.createServer(async (req, res) => {
  try {
    if (!authorized(req)) {
      res.writeHead(401, { 'WWW-Authenticate': 'Basic realm="Tidefin"' });
      return res.end('Login required');
    }
    const url = new URL(req.url, 'http://x');
    const p = url.pathname;
    const q = url.searchParams;

    if (p === '/api/health') {
      const out = { jellyfin: false, tidarr: false, tidal: false, errors: {} };
      await Promise.allSettled([
        jf('/System/Info').then(() => { out.jellyfin = true; }).catch((e) => { out.errors.jellyfin = why(e); }),
        tidarr('/api/queue/status').then((r) => { out.tidarr = r.ok; if (!r.ok) out.errors.tidarr = `HTTP ${r.status} (check TIDARR_API_KEY)`; }).catch((e) => { out.errors.tidarr = why(e); }),
        tidalAuth().then(() => { out.tidal = true; }).catch((e) => { out.errors.tidal = why(e); }),
      ]);
      out.config = { JELLYFIN_URL: JF_URL || '(empty)', TIDARR_URL: TD_URL || '(empty)', TIDAL_CLIENT_ID: TIDAL_ID ? TIDAL_ID.slice(0, 4) + '…' : '(empty)' };
      return send(res, 200, out);
    }

    // --- Jellyfin library ---
    if (p === '/api/library/albums') {
      const start = parseInt(q.get('start') || '0', 10) || 0;
      const SORTS = {
        recent: 'SortBy=DateCreated,SortName&SortOrder=Descending',
        name: 'SortBy=SortName&SortOrder=Ascending',
        artist: 'SortBy=AlbumArtist,ProductionYear,SortName&SortOrder=Ascending',
        year: 'SortBy=ProductionYear,SortName&SortOrder=Descending',
      };
      const sort = SORTS[q.get('sort')] || SORTS.recent;
      const j = await jf(`/Items?IncludeItemTypes=MusicAlbum&Recursive=true&${sort}&StartIndex=${start}&Limit=60&${jfFields}`);
      return send(res, 200, { total: j.TotalRecordCount || 0, albums: (j.Items || []).map(mapJfAlbum) });
    }
    let m;
    if ((m = /^\/api\/library\/album\/([\w-]+)$/.exec(p))) {
      const [info, tr] = await Promise.all([
        jf(`/Items?Ids=${m[1]}&${jfFields}`),
        jf(`/Items?ParentId=${m[1]}&IncludeItemTypes=Audio&Recursive=true&SortBy=ParentIndexNumber,IndexNumber,SortName&Fields=MediaSources`),
      ]);
      const a = (info.Items || [])[0];
      if (!a) return fail(res, 404, 'Album not found');
      return send(res, 200, { ...mapJfAlbum(a), tracks: (tr.Items || []).map(mapJfTrack) });
    }
    if (p === '/api/library/search') {
      const term = encodeURIComponent(q.get('q') || '');
      const [t, a] = await Promise.all([
        jf(`/Items?searchTerm=${term}&IncludeItemTypes=Audio&Recursive=true&Limit=30`),
        jf(`/Items?searchTerm=${term}&IncludeItemTypes=MusicAlbum&Recursive=true&Limit=12&${jfFields}`),
      ]);
      return send(res, 200, { tracks: (t.Items || []).map(mapJfTrack), albums: (a.Items || []).map(mapJfAlbum) });
    }
    if ((m = /^\/api\/library\/image\/([\w-]+)$/.exec(p))) {
      const up = await fetch(`${JF_URL}/Items/${m[1]}/Images/Primary?maxHeight=${parseInt(q.get('h') || '300', 10)}&quality=90`, { headers: jfHeaders() });
      if (!up.ok) return fail(res, 404, 'No image');
      return pipeUpstream(up, res);
    }
    if ((m = /^\/api\/library\/stream\/([\w-]+)$/.exec(p))) {
      const headers = jfHeaders();
      if (req.headers.range) headers.Range = req.headers.range;
      const up = await fetch(`${JF_URL}/Audio/${m[1]}/stream?static=true`, { headers });
      return pipeUpstream(up, res);
    }
    if (p === '/api/library/refresh' && req.method === 'POST') {
      await jf('/Library/Refresh', { method: 'POST' });
      return send(res, 200, { ok: true });
    }

    // --- Tidal search ---
    if (p === '/api/tidal/search') {
      const term = (q.get('q') || '').trim();
      if (!term) return send(res, 200, { tracks: [], albums: [] });
      return send(res, 200, await tidalSearch(term));
    }
    if ((m = /^\/api\/tidal\/album\/(\d+)$/.exec(p))) return send(res, 200, await tidalAlbum(m[1]));

    // --- Tidal playback through Tidarr (your Tidal account) ---
    if ((m = /^\/api\/tidal\/stream\/(\d+)$/.exec(p))) {
      const s = await tidarr(`/api/stream/sign/${m[1]}`);
      if (!s.ok) return fail(res, 502, `Tidarr stream sign failed (${s.status})`);
      const j = await s.json().catch(() => ({}));
      let target = j.url || j.signedUrl || j.streamUrl || null;
      if (!target && j.exp && j.sig) target = `/api/stream/play/${m[1]}?exp=${encodeURIComponent(j.exp)}&sig=${encodeURIComponent(j.sig)}`;
      if (!target) return fail(res, 502, 'Unexpected stream response from Tidarr');
      if (target.startsWith('/')) target = TD_URL + target;
      else { const u = new URL(target); target = TD_URL + u.pathname + u.search; } // always go via internal Tidarr URL
      const headers = { 'X-Api-Key': TD_KEY };
      if (req.headers.range) headers.Range = req.headers.range;
      const up = await fetch(target, { headers });
      if (!up.ok && up.status !== 206) return fail(res, 502, `Tidarr stream failed (${up.status})`);
      return pipeUpstream(up, res);
    }

    // --- Tidarr downloads ---
    if (p === '/api/download' && req.method === 'POST') {
      const b = await readBody(req);
      const type = b.type === 'album' ? 'album' : 'track';
      const id = String(b.id || '').replace(/\D/g, '');
      if (!id) return fail(res, 400, 'Missing Tidal id');
      const r = await tidarr('/api/save', {
        method: 'POST',
        body: JSON.stringify({ item: { id, url: `https://listen.tidal.com/${type}/${id}`, type, status: 'queue_download', title: b.title || '', artist: b.artist || '' } }),
      });
      if (!r.ok && r.status !== 201) return fail(res, 502, `Tidarr refused (${r.status}): ${(await r.text()).slice(0, 200)}`);
      return send(res, 200, { ok: true });
    }
    if (p === '/api/queue') return send(res, 200, { items: await tidarrQueue() });
    if (p === '/api/queue/retry' && req.method === 'POST') {
      const r = await tidarr('/api/retry-failed', { method: 'POST' });
      return send(res, r.ok ? 200 : 502, { ok: r.ok });
    }
    if (p === '/api/queue/clear-finished' && req.method === 'POST') {
      const r = await tidarr('/api/remove-finished', { method: 'DELETE' });
      return send(res, r.ok ? 200 : 502, { ok: r.ok });
    }

    // Same-origin proxy for Tidal cover art (lets the player read its colours)
    if (p === '/api/img') {
      let u; try { u = new URL(q.get('u') || ''); } catch { return fail(res, 400, 'Bad image URL'); }
      if (u.protocol !== 'https:' || !/(^|\.)tidal\.com$/.test(u.hostname)) return fail(res, 403, 'Only Tidal images');
      const up = await fetch(u.href);
      if (!up.ok) return fail(res, 404, 'No image');
      return pipeUpstream(up, res);
    }
    if (p.startsWith('/api/')) return fail(res, 404, 'Unknown endpoint');

    // --- static files ---
    const file = path.normalize(path.join(PUBLIC_DIR, p === '/' ? 'index.html' : p));
    if (!file.startsWith(PUBLIC_DIR)) return fail(res, 403, 'Forbidden');
    fs.readFile(file, (err, data) => {
      if (err) return fs.readFile(path.join(PUBLIC_DIR, 'index.html'), (e2, d2) => e2 ? fail(res, 404, 'Not found') : send(res, 200, d2, MIME['.html']));
      send(res, 200, data, MIME[path.extname(file)] || 'application/octet-stream');
    });
  } catch (e) {
    console.error(new Date().toISOString(), req.method, req.url, '-', why(e));
    if (!res.headersSent) fail(res, 502, why(e));
    else res.destroy();
  }
});

server.listen(PORT, () => {
  console.log(`Tidefin listening on :${PORT}`);
  console.log(`  Jellyfin: ${JF_URL || '(not set)'}  Tidarr: ${TD_URL || '(not set)'}  Tidal country: ${COUNTRY}`);
  Promise.allSettled([
    jf('/System/Info').then(() => console.log('  ✓ Jellyfin OK')).catch((e) => console.log('  ✗ Jellyfin:', why(e))),
    tidarr('/api/queue/status').then((r) => console.log(r.ok ? '  ✓ Tidarr OK' : `  ✗ Tidarr: HTTP ${r.status}`)).catch((e) => console.log('  ✗ Tidarr:', why(e))),
    tidalAuth().then(() => console.log('  ✓ Tidal OK')).catch((e) => console.log('  ✗ Tidal:', why(e))),
  ]);
});
