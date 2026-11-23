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

const jfFields = 'Fields=PrimaryImageAspectRatio,ProductionYear,ChildCount,RecursiveItemCount,DateCreated,Genres';
const mapJfAlbum = (i) => ({
  id: i.Id, title: i.Name, artist: i.AlbumArtist || (i.AlbumArtists?.[0]?.Name) || (i.Artists || []).join(', '),
  year: i.ProductionYear || null, hasImage: !!(i.ImageTags && i.ImageTags.Primary),
  tracks: i.ChildCount || i.RecursiveItemCount || null,
});
const mapJfArtist = (i) => ({ id: i.Id, name: i.Name, hasImage: !!(i.ImageTags && i.ImageTags.Primary) });

// A Jellyfin user is needed for per-user data such as play counts (top songs).
let jfUserId = null;
let libIndexCache = null;
const lyricsCache = new Map();
const artistAlbumCache = new Map(); // artist name -> {at, data}
const previewCache = new Map(); // tidal id -> preview url (or null)
async function jfUser() {
  if (jfUserId) return jfUserId;
  const users = await jf('/Users');
  const want = (process.env.JELLYFIN_USER || '').trim().toLowerCase();
  const u = (want && users.find((x) => x.Name.toLowerCase() === want)) || users.find((x) => x.Policy && x.Policy.IsAdministrator) || users[0];
  jfUserId = u ? u.Id : null;
  return jfUserId;
}
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
  // One request per type: Tidal only fills the first relationship when both are requested together.
  const searchIds = async (type) => {
    const doc = await tidal('/searchResults', [['filter[query]', q], ['explicitFilter', 'INCLUDE'], ['include', type]]);
    const res = Array.isArray(doc.data) ? doc.data[0] : doc.data;
    let ids = relIds(res, type).filter((d) => d.type === type).map((d) => d.id);
    if (!ids.length) ids = (doc.included || []).filter((d) => d.type === type).map((d) => d.id);
    return ids;
  };
  const [tIds, aIds] = await Promise.all([searchIds('tracks'), searchIds('albums')]);
  const trackIds = tIds.slice(0, 20);
  const albumIds = aIds.slice(0, 20);
  const [tDoc, aDoc] = await Promise.all([
    tidalByIds('tracks', trackIds, ['artists', 'albums', 'albums.coverArt'], ['artists', 'albums']),
    tidalByIds('albums', albumIds, ['artists', 'coverArt'], ['artists']),
  ]);
  const tMap = indexIncluded(tDoc), aMap = indexIncluded(aDoc);
  const order = (ids, list) => ids.map((id) => list.find((x) => String(x.id) === String(id))).filter(Boolean);
  const tracks = order(trackIds, tDoc.data || []).map((t) => {
    const alb = tMap.get(`albums:${relIds(t, 'albums')[0]?.id}`);
    return {
      id: t.id, isrc: t.attributes?.isrc || null, title: t.attributes?.title + (t.attributes?.version ? ` (${t.attributes.version})` : ''),
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

async function tidalArtistAlbums(name) {
  const sDoc = await tidal('/searchResults', [['filter[query]', name], ['include', 'artists']]);
  const sRes = Array.isArray(sDoc.data) ? sDoc.data[0] : sDoc.data;
  const map = indexIncluded(sDoc);
  const norm = (x) => (x || '').toLowerCase().normalize('NFKD').replace(/[^a-z0-9]/g, '');
  const cands = relIds(sRes, 'artists').map((r) => map.get(`artists:${r.id}`)).filter(Boolean);
  const artist = cands.find((a) => norm(a.attributes?.name) === norm(name));
  if (!artist) return { artist: null, albums: [] };
  const refs = [];
  let cursor = null;
  for (let page = 0; page < 6; page++) {
    const doc = await tidal(`/artists/${artist.id}/relationships/albums`, cursor ? [['page[cursor]', cursor]] : []);
    refs.push(...(doc.data || []));
    const next = doc.links?.next;
    cursor = next ? new URL(next, 'https://openapi.tidal.com').searchParams.get('page[cursor]') : null;
    if (!cursor) break;
  }
  const ids = [...new Set(refs.filter((d) => d.type === 'albums').map((d) => d.id))].slice(0, 100);
  const albums = [];
  for (let i = 0; i < ids.length; i += 20) {
    const doc = await tidalByIds('albums', ids.slice(i, i + 20), ['artists', 'coverArt'], ['artists']);
    const m = indexIncluded(doc);
    for (const a of doc.data || []) {
      albums.push({
        id: a.id, title: a.attributes?.title, artist: artistNames(m, a), type: a.attributes?.albumType || 'ALBUM',
        year: (a.attributes?.releaseDate || '').slice(0, 4) || null, date: a.attributes?.releaseDate || '',
        cover: pickCover(m, a), tracks: a.attributes?.numberOfItems || null, explicit: !!a.attributes?.explicit,
      });
    }
  }
  // Tidal often lists clean + explicit versions: keep one per title+type (prefer explicit)
  const seen = new Map();
  for (const a of albums) { const k = norm(a.title) + a.type; const o = seen.get(k); if (!o || (a.explicit && !o.explicit)) seen.set(k, a); }
  return { artist: { id: artist.id, name: artist.attributes?.name }, albums: [...seen.values()].sort((a, b) => (b.date || '').localeCompare(a.date || '')) };
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
        id: t.id, isrc: t.attributes?.isrc || null, title: t.attributes?.title + (t.attributes?.version ? ` (${t.attributes.version})` : ''),
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
    if (p === '/api/library/delete' && req.method === 'POST') {
      // Permanently deletes media files through Jellyfin. type: song | album | artist
      const b = await readBody(req);
      const id = String(b.id || '').replace(/[^\w-]/g, '');
      if (!id) return fail(res, 400, 'Missing id');
      let ids = [id];
      if (b.type === 'artist') {
        const j = await jf(`/Items?IncludeItemTypes=MusicAlbum&Recursive=true&AlbumArtistIds=${id}&Fields=Path`);
        ids = (j.Items || []).map((i) => i.Id);
        if (!ids.length) return fail(res, 404, 'No albums found for this artist');
      }
      const failed = [];
      for (const x of ids) {
        const r = await fetch(`${JF_URL}/Items/${x}`, { method: 'DELETE', headers: jfHeaders() });
        if (!r.ok) failed.push(r.status);
      }
      libIndexCache = null;
      if (failed.length) {
        const st = failed[0];
        const hint = st === 401 || st === 403 ? ' — Jellyfin refused: allow media deletion for the API key / admin user in Jellyfin.' : '';
        return fail(res, 502, `Jellyfin could not delete ${failed.length} of ${ids.length} item(s) (HTTP ${st})${hint}`);
      }
      console.log(new Date().toISOString(), 'deleted', b.type, id, `(${ids.length} item(s))`);
      return send(res, 200, { ok: true, deleted: ids.length });
    }
    if (p === '/api/lyrics') {
      // Synced lyrics from LRCLIB (free, open lyrics database).
      const artist = (q.get('artist') || '').split(',')[0].trim();
      const title = (q.get('title') || '').replace(/\s*[\(\[](feat|ft|with)\.?[^\)\]]*[\)\]]/ig, '').trim();
      const album = q.get('album') || '', dur = Math.round(+q.get('duration') || 0);
      const key = [artist, title, album, dur].join('|').toLowerCase();
      if (lyricsCache.has(key)) return send(res, 200, lyricsCache.get(key));
      const H = { 'User-Agent': 'Tidefin/1.0 (https://github.com/LankyFranky89/tidefin)' };
      let hit = null;
      try {
        const qs = new URLSearchParams({ artist_name: artist, track_name: title, album_name: album });
        if (dur) qs.set('duration', String(dur));
        const r = await fetch('https://lrclib.net/api/get?' + qs, { headers: H });
        if (r.ok) hit = await r.json();
        if (!hit || (!hit.syncedLyrics && !hit.plainLyrics)) {
          const r2 = await fetch('https://lrclib.net/api/search?' + new URLSearchParams({ track_name: title, artist_name: artist }), { headers: H });
          const list = r2.ok ? await r2.json() : [];
          hit = list.find((x) => x.syncedLyrics) || list[0] || null;
        }
      } catch (e) { console.error('lyrics lookup failed', e.message); }
      const out = { synced: (hit && hit.syncedLyrics) || null, plain: (hit && hit.plainLyrics) || null, instrumental: !!(hit && hit.instrumental) };
      if (lyricsCache.size > 500) lyricsCache.clear();
      lyricsCache.set(key, out);
      return send(res, 200, out);
    }
    if (p === '/api/home/new') {
      // Tidal releases from the last 12 months by artists you have, that you don't own yet.
      const names = (q.get('artists') || '').split('|').map((x) => x.trim()).filter(Boolean).slice(0, 10);
      const norm = (x) => String(x || '').toLowerCase().normalize('NFKD').replace(/[̀-ͯ]/g, '').replace(/\(.*?\)|\[.*?\]/g, '').replace(/[^a-z0-9]/g, '');
      const owned = new Set(((libIndexCache && libIndexCache.albums) || []).map(([ar, t]) => norm(ar) + '|' + norm(t)));
      const cutoff = new Date(Date.now() - 365 * 864e5).toISOString().slice(0, 10);
      const out = [];
      for (const n of names) { // one artist at a time: Tidal rate-limits bursts
        try {
          let c = artistAlbumCache.get(n.toLowerCase());
          if (!c || Date.now() - c.at > 6 * 3600e3) { c = { at: Date.now(), data: await tidalArtistAlbums(n) }; artistAlbumCache.set(n.toLowerCase(), c); }
          for (const al of c.data.albums || []) {
            if ((al.date || '') < cutoff || al.type === 'SINGLE') continue;
            if (owned.has(norm(n) + '|' + norm(al.title))) continue;
            out.push(al);
          }
        } catch (e) { console.error('new releases:', n, e.message); }
      }
      out.sort((x, y) => (y.date || '').localeCompare(x.date || ''));
      return send(res, 200, { albums: out.slice(0, 24) });
    }
    if (p === '/api/library/index') {
      // Compact list of what's in the library, so the UI can mark Tidal items you already have.
      if (!libIndexCache || Date.now() - libIndexCache.at > 60000 || q.get('fresh')) {
        const [al, tr] = await Promise.all([
          jf('/Items?IncludeItemTypes=MusicAlbum&Recursive=true&Fields=AlbumArtist&EnableImages=false&Limit=20000'),
          jf('/Items?IncludeItemTypes=Audio&Recursive=true&Fields=AlbumArtist&Limit=50000'),
        ]);
        libIndexCache = {
          at: Date.now(),
          albums: (() => {
            // how many songs of each album you actually have (one downloaded song != whole album)
            const per = new Map();
            for (const t of tr.Items || []) if (t.AlbumId) per.set(t.AlbumId, (per.get(t.AlbumId) || 0) + 1);
            return (al.Items || []).map((i) => [i.AlbumArtist || (i.Artists || [])[0] || '', i.Name, per.get(i.Id) || 0]);
          })(),
          tracks: (tr.Items || []).map((i) => [(i.Artists || [])[0] || i.AlbumArtist || '', i.Name, i.Id, i.Album || '', (i.ImageTags && i.ImageTags.Primary) ? i.Id : (i.AlbumPrimaryImageTag ? i.AlbumId : null), i.RunTimeTicks ? Math.round(i.RunTimeTicks / 1e7) : null]),
        };
      }
      return send(res, 200, { albums: libIndexCache.albums, tracks: libIndexCache.tracks });
    }
    if (p === '/api/library/artists') {
      const start = parseInt(q.get('start') || '0', 10) || 0;
      const j = await jf(`/Artists/AlbumArtists?SortBy=SortName&SortOrder=Ascending&StartIndex=${start}&Limit=120&Fields=PrimaryImageAspectRatio&EnableImages=true`);
      return send(res, 200, { total: j.TotalRecordCount || 0, artists: (j.Items || []).map(mapJfArtist) });
    }
    if ((m = /^\/api\/library\/artist\/([\w-]+)$/.exec(p))) {
      const id = m[1];
      const uid = await jfUser().catch(() => null);
      const [info, albums, top] = await Promise.all([
        jf(`/Items?Ids=${id}&Fields=Overview,Genres`),
        jf(`/Items?IncludeItemTypes=MusicAlbum&Recursive=true&AlbumArtistIds=${id}&SortBy=ProductionYear,PremiereDate,SortName&SortOrder=Descending&${jfFields}`),
        uid ? jf(`/Users/${uid}/Items?IncludeItemTypes=Audio&Recursive=true&ArtistIds=${id}&SortBy=PlayCount,SortName&SortOrder=Descending&Limit=10&Fields=UserData`).catch(() => ({ Items: [] })) : { Items: [] },
      ]);
      const a = (info.Items || [])[0];
      if (!a) return fail(res, 404, 'Artist not found');
      const topItems = (top.Items || []);
      const played = topItems.some((t) => t.UserData && t.UserData.PlayCount > 0);
      return send(res, 200, {
        ...mapJfArtist(a), genres: a.Genres || [],
        albums: (albums.Items || []).map(mapJfAlbum),
        topSongs: topItems.map((t) => ({ ...mapJfTrack(t), plays: (t.UserData && t.UserData.PlayCount) || 0 })),
        topByPlays: played,
      });
    }
    if ((m = /^\/api\/library\/artist\/([\w-]+)\/tracks$/.exec(p))) {
      const j = await jf(`/Items?IncludeItemTypes=Audio&Recursive=true&ArtistIds=${m[1]}&SortBy=ProductionYear,Album,ParentIndexNumber,IndexNumber&SortOrder=Ascending&Limit=2000`);
      return send(res, 200, { tracks: (j.Items || []).map(mapJfTrack) });
    }
    if (p === '/api/library/songs') {
      const start = parseInt(q.get('start') || '0', 10) || 0;
      const SORTS = {
        title: 'SortBy=SortName&SortOrder=Ascending',
        artist: 'SortBy=AlbumArtist,Album,ParentIndexNumber,IndexNumber&SortOrder=Ascending',
        album: 'SortBy=Album,ParentIndexNumber,IndexNumber&SortOrder=Ascending',
        recent: 'SortBy=DateCreated,SortName&SortOrder=Descending',
      };
      const j = await jf(`/Items?IncludeItemTypes=Audio&Recursive=true&${SORTS[q.get('sort')] || SORTS.title}&StartIndex=${start}&Limit=150`);
      return send(res, 200, { total: j.TotalRecordCount || 0, tracks: (j.Items || []).map(mapJfTrack) });
    }
    if (p === '/api/library/genres') {
      const j = await jf('/MusicGenres?SortBy=SortName&SortOrder=Ascending&Recursive=true&Fields=ItemCounts');
      return send(res, 200, { genres: (j.Items || []).map((g) => ({ id: g.Id, name: g.Name, albums: g.AlbumCount || null, songs: g.SongCount || null })) });
    }
    if ((m = /^\/api\/library\/genre\/([\w-]+)$/.exec(p))) {
      const j = await jf(`/Items?IncludeItemTypes=MusicAlbum&Recursive=true&GenreIds=${m[1]}&SortBy=AlbumArtist,ProductionYear,SortName&SortOrder=Ascending&${jfFields}`);
      return send(res, 200, { albums: (j.Items || []).map(mapJfAlbum) });
    }
    if (p === '/api/tidal/artist') {
      const name = (q.get('name') || '').trim();
      if (!name) return send(res, 200, { artist: null, albums: [] });
      return send(res, 200, await tidalArtistAlbums(name));
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
    if (p === '/api/tidal/debug') {
      const type = q.get('type') || 'albums';
      const doc = await tidal('/searchResults', [['filter[query]', q.get('q') || ''], ['explicitFilter', 'INCLUDE'], ['include', type]]);
      const res0 = Array.isArray(doc.data) ? doc.data[0] : doc.data;
      return send(res, 200, {
        version: 'search-split-1', dataIsArray: Array.isArray(doc.data), dataType: res0 && res0.type, dataId: res0 && res0.id,
        relKeys: res0 && res0.relationships ? Object.keys(res0.relationships) : null,
        rel: res0 && res0.relationships && res0.relationships[type],
        includedTypes: (doc.included || []).reduce((o, x) => { o[x.type] = (o[x.type] || 0) + 1; return o; }, {}),
      });
    }
    if ((m = /^\/api\/tidal\/album\/(\d+)$/.exec(p))) return send(res, 200, await tidalAlbum(m[1]));

    // --- 30-second previews: Deezer (by ISRC) -> Apple (by name) -> Tidarr fallback ---
    if ((m = /^\/api\/tidal\/preview\/(\d+)$/.exec(p))) {
      const key = m[1];
      let url = previewCache.get(key);
      if (url === undefined) {
        url = null;
        const isrc = (q.get('isrc') || '').replace(/[^A-Za-z0-9]/g, '');
        const artist = q.get('artist') || '', title = (q.get('title') || '').replace(/\s*[\(\[].*?[\)\]]/g, '');
        try {
          if (isrc) {
            const d = await fetch(`https://api.deezer.com/2.0/track/isrc:${isrc}`).then((r) => r.json());
            if (d && d.preview) url = d.preview;
          }
          if (!url && artist && title) {
            const d = await fetch(`https://api.deezer.com/search?q=${encodeURIComponent(`artist:"${artist.split(',')[0]}" track:"${title}"`)}&limit=1`).then((r) => r.json());
            if (d && d.data && d.data[0] && d.data[0].preview) url = d.data[0].preview;
          }
          if (!url && artist && title) {
            const d = await fetch(`https://itunes.apple.com/search?media=music&entity=song&limit=1&term=${encodeURIComponent(artist.split(',')[0] + ' ' + title)}`).then((r) => r.json());
            if (d && d.results && d.results[0] && d.results[0].previewUrl) url = d.results[0].previewUrl;
          }
        } catch (e) { console.error('preview lookup failed', e.message); }
        previewCache.set(key, url);
      }
      if (!url) { res.writeHead(302, { Location: `/api/tidal/stream/${key}` }); return res.end(); }
      const headers = {};
      if (req.headers.range) headers.Range = req.headers.range;
      const up = await fetch(url, { headers });
      if (!up.ok && up.status !== 206) { previewCache.delete(key); return fail(res, 502, `Preview source failed (${up.status})`); }
      return pipeUpstream(up, res);
    }

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
