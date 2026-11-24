// Tidefin — Jellyfin music player with Tidal search + Tidarr downloads.
// No npm dependencies: runs on the stock node:22-alpine image.
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const { Readable } = require('stream');
const { execFile } = require('child_process');

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
const METUBE_URL = env('METUBE_URL', 'http://127.0.0.1:8081').replace(/\/+$/, '');
const METUBE_DIR = env('METUBE_DIR', '/metube');   // MeTube's download folder, mounted into Tidefin
const MUSIC_DIR = env('MUSIC_DIR', '/music');      // your music library (same folder Jellyfin reads)
const PUBLIC_DIR = path.join(__dirname, 'public');

// ---------- settings (saved on the NAS, shared by every device) ----------
const DATA_DIR = path.join(__dirname, 'data');
const SETTINGS_FILE = path.join(DATA_DIR, 'settings.json');
const DEFAULT_SETTINGS = {
  youtube: false,                 // YouTube Music source (via MeTube)
  accent: 'cover',                // 'cover' or a hex colour
  glass: 'strong',                // off | subtle | strong
  textSize: 'normal',             // normal | large | xl
  startPage: 'home',              // home | library | search
  libraryTab: 'artists',          // artists | albums | songs | genres
  homeShelves: ['recent', 'added', 'new', 'artists'], // order = display order; missing = hidden
  ownedPct: 80,                   // album counts as owned at this % of its songs
  releaseTypes: ['studio'],       // studio, ep, single, live, compilation, deluxe
  confirmBig: true,               // ask before album / discography downloads
  lookbackMonths: 12,             // new-release shelf
};
let settings = { ...DEFAULT_SETTINGS };
try { settings = { ...DEFAULT_SETTINGS, ...JSON.parse(fs.readFileSync(SETTINGS_FILE, 'utf8')) }; } catch {}
function saveSettings(patch) {
  const allowed = Object.keys(DEFAULT_SETTINGS);
  for (const [k, v] of Object.entries(patch || {})) {
    if (!allowed.includes(k)) continue;
    if (typeof v !== typeof DEFAULT_SETTINGS[k] && !(Array.isArray(DEFAULT_SETTINGS[k]) && Array.isArray(v))) continue;
    settings[k] = v;
  }
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); fs.writeFileSync(SETTINGS_FILE, JSON.stringify(settings, null, 2)); }
  catch (e) { console.error('could not save settings:', e.message); }
  return settings;
}

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
// Collab releases show the full credit ("Antibody & Grendel"); normal albums just the album artist
const albumCredit = (i) => {
  const aa = i.AlbumArtist || (i.AlbumArtists?.[0]?.Name) || '';
  const names = []; for (const n of [...(i.Artists || []), aa]) if (n && !names.some((x) => snorm(x) === snorm(n))) names.push(n);
  return names.length > 1 && names.length <= 3 ? names.join(' & ') : (aa || names.join(', '));
};
const mapJfAlbum = (i) => ({
  id: i.Id, title: i.Name, artist: albumCredit(i),
  year: i.ProductionYear || null, hasImage: !!(i.ImageTags && i.ImageTags.Primary),
  tracks: i.ChildCount || i.RecursiveItemCount || null,
});
const mapJfArtist = (i) => ({ id: i.Id, name: i.Name, hasImage: !!(i.ImageTags && i.ImageTags.Primary) });

// A Jellyfin user is needed for per-user data such as play counts (top songs).
let jfUserId = null;
let libIndexCache = null;
// Songs downloaded one at a time ("singles"). Albums that hold only such songs stay out of Albums.
const SINGLES_FILE = '/app/data/singles.json';
const snorm = (x) => String(x || '').toLowerCase().normalize('NFKD').replace(/[\u0300-\u036f]/g, '').replace(/\(.*?\)|\[.*?\]/g, '').replace(/[^a-z0-9]/g, '');
const skey = (artist, title) => snorm(artist) + '|' + snorm(title);
let singles = [];
try { singles = JSON.parse(fs.readFileSync(SINGLES_FILE, 'utf8')); } catch {}
let singleKeys = new Set(singles.map((x) => skey(x.artist, x.title)));
function addSingle(artist, title) {
  if (!title) return;
  const k = skey(artist, title);
  if (singleKeys.has(k)) return;
  singles.push({ artist: artist || '', title, at: Date.now() }); singleKeys.add(k);
  try { fs.mkdirSync('/app/data', { recursive: true }); fs.writeFileSync(SINGLES_FILE, JSON.stringify(singles)); } catch (e) { console.error('singles save:', e.message); }
}
async function getLibIndex(fresh) {
  if (!libIndexCache || Date.now() - libIndexCache.at > 60000 || fresh) {
    const [al, tr] = await Promise.all([
      jf('/Items?IncludeItemTypes=MusicAlbum&Recursive=true&Fields=AlbumArtist&EnableImages=false&Limit=20000'),
      jf('/Items?IncludeItemTypes=Audio&Recursive=true&Fields=AlbumArtist&Limit=50000'),
    ]);
    const per = new Map(), allSingle = new Map();
    for (const t of tr.Items || []) {
      if (!t.AlbumId) continue;
      per.set(t.AlbumId, (per.get(t.AlbumId) || 0) + 1);
      const isS = singleKeys.has(skey((t.Artists || [])[0] || t.AlbumArtist, t.Name)) || singleKeys.has(skey(t.AlbumArtist, t.Name));
      allSingle.set(t.AlbumId, (allSingle.get(t.AlbumId) ?? true) && isS);
    }
    const aaSongs = new Map(), aaAlb = new Map();
    for (const t of tr.Items || []) { const k = snorm(t.AlbumArtist); if (!k) continue; aaSongs.set(k, (aaSongs.get(k) || 0) + 1); const ak = k + '|' + t.AlbumId; aaAlb.set(ak, (aaAlb.get(ak) || 0) + 1); }
    const realAA = new Set([...aaAlb].filter(([, n]) => n >= 3).map(([k]) => k.split('|')[0]));
    libIndexCache = {
      at: Date.now(),
      smallArtists: new Set([...aaSongs].filter(([k, n]) => n <= 2 && !realAA.has(k)).map(([k]) => k)),
      loose: [...allSingle].filter(([, v]) => v).map(([k]) => k),
      albums: (al.Items || []).map((i) => [i.AlbumArtist || (i.Artists || [])[0] || '', i.Name, per.get(i.Id) || 0]),
      tracks: (tr.Items || []).map((i) => [(i.Artists || [])[0] || i.AlbumArtist || '', i.Name, i.Id, i.Album || '', (i.ImageTags && i.ImageTags.Primary) ? i.Id : (i.AlbumPrimaryImageTag ? i.AlbumId : null), i.RunTimeTicks ? Math.round(i.RunTimeTicks / 1e7) : null]),
    };
  }
  return libIndexCache;
}
const looseExclude = async () => { const l = (await getLibIndex().catch(() => ({ loose: [] }))).loose; return l.length ? '&ExcludeItemIds=' + l.join(',') : ''; };
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

// Which kind of release is this? (studio / ep / single / live / compilation / deluxe)
const LIVE_RE = /\b(live|unplugged|in concert|concert|on stage|bootleg|tour edition)\b/i;
const COMP_RE = /\b(best of|greatest hits|the very best|collection|anthology|essentials?|the hits|b-sides|rarities|demos?|remixe?s|karaoke|instrumentals?)\b/i;
const DELUXE_RE = /\b(deluxe|expanded|anniversary|bonus|special edition|remaster(ed)?|reissue)\b/i;
function releaseKind(a) {
  if (a.type === 'SINGLE') return 'single';
  if (a.type === 'EP') return 'ep';
  if (LIVE_RE.test(a.title || '')) return 'live';
  if (COMP_RE.test(a.title || '')) return 'compilation';
  if (DELUXE_RE.test(a.title || '')) return 'deluxe';
  return 'studio';
}
function releaseAllowed(a, types) { const k = releaseKind(a); return types.includes(k) || (k === 'deluxe' && types.includes('studio') && !types.includes('nodeluxe')); }

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

// ---------- YouTube Music (search) + MeTube (downloads) ----------
const YT_SONGS = 'EgWKAQIIAWoMEA4QChADEAQQCRAF'; // "Songs" filter (same as ytmusicapi)
const YT_ALBUMS = 'EgWKAQIYAWoMEA4QChADEAQQCRAF'; // "Albums" filter
async function ytApi(endpoint, body) {
  const r = await fetch(`https://music.youtube.com/youtubei/v1/${endpoint}?prettyPrint=false`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0', Origin: 'https://music.youtube.com' },
    body: JSON.stringify({ ...body, context: { client: { clientName: 'WEB_REMIX', clientVersion: '1.20241106.01.00', hl: 'en', gl: 'NO' } } }),
  });
  if (!r.ok) throw new Error(`YouTube Music ${r.status}`);
  return r.json();
}
const ytText = (runs) => (runs || []).map((x) => x.text).join('');
const ytPage = (x) => x?.navigationEndpoint?.browseEndpoint?.browseEndpointContextSupportedConfigs?.browseEndpointContextMusicConfig?.pageType;
const ytThumb = (t) => { const a = t?.musicThumbnailRenderer?.thumbnail?.thumbnails || t?.croppedSquareThumbnailRenderer?.thumbnail?.thumbnails || []; const u = (a[a.length - 1] || {}).url || null; return u ? u.replace(/=w\d+-h\d+/, '=w544-h544') : null; };
function ytFind(o, key, out = []) { if (!o || typeof o !== 'object') return out; if (o[key]) out.push(o[key]); for (const k in o) if (k !== key) ytFind(o[k], key, out); return out; }
async function ytSearchAlbums(q) {
  const j = await ytApi('search', { query: q, params: YT_ALBUMS });
  return ytFind(j.contents, 'musicResponsiveListItemRenderer').map((it) => {
    const cols = (it.flexColumns || []).map((c) => c.musicResponsiveListItemFlexColumnRenderer?.text?.runs || []);
    const browseId = it.navigationEndpoint?.browseEndpoint?.browseId;
    if (!browseId || !cols[0][0]) return null;
    const meta = cols[1] || [];
    const parts = ytText(meta).split(' • ');
    const artist = meta.filter((x) => ytPage(x) === 'MUSIC_PAGE_TYPE_ARTIST').map((x) => x.text).join(', ') || parts[1] || '';
    return { id: browseId, title: cols[0][0].text, artist, kind: parts[0] || 'Album', year: (parts.find((x) => /^\d{4}$/.test(x.trim())) || '').trim() || null, cover: ytThumb(it.thumbnail) };
  }).filter(Boolean).slice(0, 16);
}
// Full albums uploaded by other channels (community playlists), e.g. rare or out-of-print releases
const YT_PLAYLISTS = 'EgeKAQQoAEABagwQDhAKEAMQBBAJEAU='; // "Community playlists" filter
async function ytSearchUploads(q) {
  const j = await ytApi('search', { query: q + ' full album', params: YT_PLAYLISTS });
  return ytFind(j.contents, 'musicResponsiveListItemRenderer').map((it) => {
    const cols = (it.flexColumns || []).map((c) => c.musicResponsiveListItemFlexColumnRenderer?.text?.runs || []);
    const browseId = it.navigationEndpoint?.browseEndpoint?.browseId;
    if (!browseId || !cols[0] || !cols[0][0]) return null;
    const title = cols[0][0].text;
    if (!/album|full|\bep\b|discography/i.test(title)) return null;
    const parts = ytText(cols[1] || []).split(' • ');
    const uploader = parts.length > 1 ? parts[1] : parts[0] || '';
    return { id: browseId, title, artist: uploader, uploader, kind: 'Upload', year: (title.match(/\b(19|20)\d{2}\b/) || [])[0] || null, cover: ytThumb(it.thumbnail) };
  }).filter(Boolean).slice(0, 10);
}
async function ytAlbum(browseId) {
  const j = await ytApi('browse', { browseId });
  const hdr = ytFind(j, 'musicResponsiveHeaderRenderer')[0] || ytFind(j, 'musicDetailHeaderRenderer')[0] || {};
  const title = ytText(hdr.title?.runs);
  const strap = hdr.straplineTextOne?.runs || hdr.subtitle?.runs || [];
  const artist = strap.filter((x) => ytPage(x) === 'MUSIC_PAGE_TYPE_ARTIST').map((x) => x.text).join(', ') || ytText(strap).split(' • ').find((x) => x && !/^(album|single|ep|\d{4})$/i.test(x.trim())) || '';
  const sub = ytText(hdr.subtitle?.runs);
  const year = (sub.match(/\b(19|20)\d{2}\b/) || [])[0] || null;
  const cover = ytThumb(hdr.thumbnail?.musicThumbnailRenderer ? hdr.thumbnail : hdr.thumbnail?.croppedSquareThumbnailRenderer ? hdr.thumbnail : null) || ytThumb(ytFind(j, 'musicThumbnailRenderer')[0] ? { musicThumbnailRenderer: ytFind(j, 'musicThumbnailRenderer')[0] } : null);
  const playlistId = ytFind(j, 'audioPlaylistId')[0] || ytFind(j, 'playlistId')[0] || null;
  const tracks = ytFind(j, 'musicResponsiveListItemRenderer').map((it, i) => {
    const cols = (it.flexColumns || []).map((c) => c.musicResponsiveListItemFlexColumnRenderer?.text?.runs || []);
    const videoId = it.playlistItemData?.videoId;
    if (!videoId || !cols[0] || !cols[0][0]) return null;
    const dur = ytText(it.fixedColumns?.[0]?.musicResponsiveListItemFixedColumnRenderer?.text?.runs);
    const tArtist = (cols[1] || []).filter((x) => ytPage(x) === 'MUSIC_PAGE_TYPE_ARTIST').map((x) => x.text).join(', ');
    return { id: videoId, title: cols[0][0].text, artist: tArtist || artist, album: title, number: +ytText(it.index?.runs) || i + 1,
      duration: dur ? dur.split(':').reduce((a, b) => a * 60 + +b, 0) : null, cover };
  }).filter(Boolean);
  return { id: browseId, title, artist, year, cover, playlistId, tracks };
}

async function ytSearch(q) {
  const r = await fetch('https://music.youtube.com/youtubei/v1/search?prettyPrint=false', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0', Origin: 'https://music.youtube.com' },
    body: JSON.stringify({ query: q, params: YT_SONGS, context: { client: { clientName: 'WEB_REMIX', clientVersion: '1.20241106.01.00', hl: 'en', gl: 'NO' } } }),
  });
  if (!r.ok) throw new Error(`YouTube Music ${r.status}`);
  const j = await r.json();
  const out = [];
  const walk = (o) => {
    if (!o || typeof o !== 'object') return;
    if (o.musicResponsiveListItemRenderer) {
      const it = o.musicResponsiveListItemRenderer;
      const cols = (it.flexColumns || []).map((c) => (c.musicResponsiveListItemFlexColumnRenderer?.text?.runs || []));
      const videoId = it.playlistItemData?.videoId || it.overlay?.musicItemThumbnailOverlayRenderer?.content?.musicPlayButtonRenderer?.playNavigationEndpoint?.watchEndpoint?.videoId;
      if (videoId && cols[0] && cols[0][0]) {
        const meta = (cols[1] || []).map((x) => x.text);
        const parts = meta.join('').split(' • ');
        const artist = (cols[1] || []).filter((x) => x.navigationEndpoint?.browseEndpoint?.browseEndpointContextSupportedConfigs?.browseEndpointContextMusicConfig?.pageType === 'MUSIC_PAGE_TYPE_ARTIST').map((x) => x.text).join(', ') || parts[0] || '';
        const album = (cols[1] || []).find((x) => x.navigationEndpoint?.browseEndpoint?.browseEndpointContextSupportedConfigs?.browseEndpointContextMusicConfig?.pageType === 'MUSIC_PAGE_TYPE_ALBUM')?.text || '';
        const dur = parts.find((x) => /^\d+:\d{2}(:\d{2})?$/.test(x.trim())) || '';
        const thumbs = it.thumbnail?.musicThumbnailRenderer?.thumbnail?.thumbnails || [];
        const sec = dur ? dur.trim().split(':').reduce((a, b) => a * 60 + +b, 0) : null;
        out.push({ id: videoId, title: cols[0][0].text, artist, album, duration: sec, cover: (thumbs[thumbs.length - 1] || {}).url || null });
      }
      return;
    }
    for (const k in o) walk(o[k]);
  };
  walk(j.contents);
  return out.slice(0, 20);
}

// jobs we sent to MeTube: videoId -> {artist, album, title, state}
const ytJobs = new Map();
const YT_JOBS_FILE = path.join(DATA_DIR, 'youtube-jobs.json');
try { for (const [k, v] of Object.entries(JSON.parse(fs.readFileSync(YT_JOBS_FILE, 'utf8')))) ytJobs.set(k, v); } catch {}
const saveYtJobs = () => { try { fs.mkdirSync(DATA_DIR, { recursive: true }); fs.writeFileSync(YT_JOBS_FILE, JSON.stringify(Object.fromEntries(ytJobs))); } catch {} };
const safeName = (x) => String(x || 'Unknown').replace(/[\/\\:*?"<>|]/g, '_').replace(/\s+/g, ' ').trim().slice(0, 120) || 'Unknown';

async function ytDownload(job) {
  const r = await fetch(`${METUBE_URL}/add`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ url: `https://music.youtube.com/watch?v=${job.id}`, quality: 'best', format: 'm4a', auto_start: true }),
  });
  if (!r.ok) throw new Error(`MeTube refused (${r.status})`);
  ytJobs.set(job.id, { ...job, state: 'queued', at: Date.now() }); saveYtJobs();
}

// ---------- tags: write correct names, numbers and cover into YouTube downloads (ffmpeg) ----------
let ffmpegReady = null;
function ensureFfmpeg() {
  if (!ffmpegReady) ffmpegReady = new Promise((res) => {
    execFile('ffmpeg', ['-version'], (e) => {
      if (!e) return res(true);
      console.log('installing ffmpeg for tagging…');
      execFile('apk', ['add', '--no-cache', 'ffmpeg'], { timeout: 300000 }, (e2) => {
        if (e2) { console.error('ffmpeg install failed:', e2.message); ffmpegReady = null; return res(false); }
        res(true);
      });
    });
  });
  return ffmpegReady;
}
const run = (cmd, args) => new Promise((res, rej) => execFile(cmd, args, { timeout: 120000 }, (e, so, se) => e ? rej(new Error((se || e.message).slice(-300))) : res()));
async function tagFile(file, job) {
  if (!(await ensureFfmpeg())) return;
  const dir = path.dirname(file);
  let cov = null;
  for (const u of [job.cover, job.coverFallback].filter(Boolean)) {
    if (cov) break;
    try {
      const r = await fetch(u);
      if (r.ok) {
        cov = file + '.cover.jpg'; fs.writeFileSync(cov, Buffer.from(await r.arrayBuffer()));
        const folderCov = path.join(dir, 'cover.jpg'); if (!fs.existsSync(folderCov)) fs.copyFileSync(cov, folderCov);
      }
    } catch {}
  }
  const tmp = file + '.tagging' + path.extname(file);
  const md = { title: job.title, artist: job.artist, album_artist: job.albumArtist || job.artist, album: job.album, date: job.year, track: job.number ? (job.total ? `${job.number}/${job.total}` : String(job.number)) : null };
  const args = ['-y', '-loglevel', 'error', '-i', file];
  if (cov) args.push('-i', cov, '-map', '0:a', '-map', '1:v', '-c', 'copy', '-disposition:v:0', 'attached_pic');
  else args.push('-map', '0:a', '-c', 'copy');
  for (const [k, v] of Object.entries(md)) if (v) args.push('-metadata', `${k}=${v}`);
  args.push(tmp);
  try { await run('ffmpeg', args); fs.renameSync(tmp, file); }
  catch (e) { console.error('tagging failed:', path.basename(file), e.message); try { fs.unlinkSync(tmp); } catch {} }
  if (cov) try { fs.unlinkSync(cov); } catch {}
}

// ---------- MusicBrainz: match fan-uploaded albums to the official tracklist ----------
const MB_UA = 'Tidefin/1.0 ( https://github.com/LankyFranky89/tidefin )';
let mbLast = 0;
async function mb(p) {
  const wait = mbLast + 1100 - Date.now(); if (wait > 0) await new Promise((r) => setTimeout(r, wait)); // 1 request/second
  mbLast = Date.now();
  const r = await fetch('https://musicbrainz.org/ws/2/' + p + (p.includes('?') ? '&' : '?') + 'fmt=json', { headers: { 'User-Agent': MB_UA, Accept: 'application/json' } });
  if (!r.ok) throw new Error('MusicBrainz ' + r.status);
  return r.json();
}
const words = (x) => String(x || '').toLowerCase().normalize('NFKD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9]+/g, ' ').trim().split(' ').filter(Boolean);
function sim(a, b) { // word overlap 0..1
  const A = words(a), B = words(b); if (!A.length || !B.length) return 0;
  const sb = new Set(B); const hit = A.filter((w) => sb.has(w)).length;
  return (2 * hit) / (A.length + B.length);
}
function cleanTitle(t, artist) {
  let x = String(t || '');
  x = x.replace(/[([](official|lyric|lyrics|audio|video|hd|hq|4k|remaster(ed)?|visuali[sz]er|full album|explicit)[^)\]]*[)\]]/gi, '');
  x = x.replace(/^\s*(track\s*)?\d{1,2}\s*[.)\-:]\s*/i, '');
  for (const a of String(artist || '').split(',')) { const n = a.trim(); if (n) x = x.replace(new RegExp('^\\s*' + n.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\s*[-–—:|]\\s*', 'i'), ''); }
  return x.replace(/[([]\s*[)\]]/g, '').replace(/\s{2,}/g, ' ').replace(/\s*[-–—|]\s*$/, '').trim() || String(t || '');
}
function cleanAlbum(t, artist) {
  return cleanTitle(String(t || '').replace(/\b(full\s*album|complete\s*album|full\s*ep|album\s*completo)\b/gi, '').replace(/[([]\s*(19|20)\d{2}\s*[)\]]/g, ''), artist);
}
// "Artist - Album [Full Album]" -> {artist, album}; the uploader's name is often not the band
function splitUpload(title, artistGuess) {
  const t = String(title || '').replace(/\b(full\s*album|complete\s*album|full\s*ep|album\s*completo|hq|hd)\b/gi, '').replace(/[([]\s*(19|20)\d{2}\s*[)\]]/g, '').replace(/[([]\s*[)\]]/g, '');
  const m = t.match(/^\s*(.+?)\s+[-–—|:]\s+(.+?)\s*$/);
  if (m) return { artist: m[1].trim(), album: cleanAlbum(m[2], m[1]) };
  return { artist: artistGuess || '', album: cleanAlbum(t, artistGuess) };
}
function scoreCandidate(c, ytTitles, artist) {
  const map = ytTitles.map((yt, i) => {
    const cl = cleanTitle(yt, artist); let best = null, bs = 0;
    for (const t of c.tracks) { const v = sim(cl, t.title); if (v > bs) { bs = v; best = t; } }
    if (bs < 0.5 && c.tracks.length === ytTitles.length) { best = c.tracks[i]; bs = 0.5; } // same length: go by position
    return best && bs >= 0.5 ? { n: best.n, title: best.title, score: +bs.toFixed(2) } : null;
  });
  const matched = map.filter(Boolean);
  return { ...c, total: c.tracks.length, map, trackScore: +(matched.length ? matched.reduce((x, y) => x + y.score, 0) / ytTitles.length : 0).toFixed(2) };
}
// Sources, best first: Tidal (exact official tracklists), Deezer (free), MusicBrainz (rare releases)
async function srcTidal(artist, album) {
  const r = await tidalSearch(`${artist} ${album}`.trim());
  const hits = r.albums.filter((a) => sim(a.title, album) >= 0.6 && (!artist || sim(a.artist, artist) >= 0.5)).slice(0, 2);
  const out = [];
  for (const h of hits) {
    const d = await tidalAlbum(h.id);
    out.push({ source: 'Tidal', id: 'tidal:' + h.id, title: d.title, artist: d.artist, year: d.year, cover: d.cover, tracks: d.tracks.map((t, i) => ({ n: t.number || i + 1, title: t.title })) });
  }
  return out;
}
async function srcDeezer(artist, album) {
  const q = artist ? `artist:"${artist}" album:"${album}"` : album;
  const s = await (await fetch('https://api.deezer.com/search/album?limit=8&q=' + encodeURIComponent(q))).json();
  const hits = (s.data || []).filter((a) => sim(a.title, album) >= 0.6 && (!artist || sim(a.artist?.name, artist) >= 0.5)).slice(0, 2);
  const out = [];
  for (const h of hits) {
    const d = await (await fetch('https://api.deezer.com/album/' + h.id)).json();
    out.push({ source: 'Deezer', id: 'deezer:' + h.id, title: d.title, artist: d.artist?.name || h.artist?.name, year: (d.release_date || '').slice(0, 4) || null, cover: d.cover_xl || d.cover_big || null,
      tracks: ((d.tracks && d.tracks.data) || []).map((t, i) => ({ n: i + 1, title: t.title })) });
  }
  return out;
}
async function srcMusicBrainz(artist, album, count) {
  const search = async (q) => (await mb('release/?limit=10&query=' + encodeURIComponent(q))).releases || [];
  let rels = await search(`release:"${album.replace(/"/g, '')}"` + (artist ? ` AND artist:"${artist.replace(/"/g, '')}"` : ''));
  if (!rels.length && artist) rels = await search(`release:"${album.replace(/"/g, '')}"`); // artist name may be off
  const list = rels.map((r) => ({ id: r.id, title: r.title, artist: (r['artist-credit'] || []).map((c) => c.name + (c.joinphrase || '')).join(''), year: (r.date || '').slice(0, 4) || null, count: r['track-count'] || 0, score: r.score || 0 }))
    .filter((r) => sim(r.title, album) >= 0.5 && (!artist || sim(r.artist, artist) >= 0.4));
  list.sort((a, b) => (Math.abs(a.count - count) - Math.abs(b.count - count)) || (b.score - a.score));
  const out = [];
  for (const r of list.slice(0, 2)) {
    const d = await mb(`release/${r.id}?inc=recordings+artist-credits`);
    const tracks = []; for (const m of d.media || []) for (const t of m.tracks || []) tracks.push({ n: tracks.length + 1, title: t.title });
    out.push({ source: 'MusicBrainz', id: 'mb:' + r.id, title: r.title, artist: r.artist, year: r.year, cover: `https://coverartarchive.org/release/${r.id}/front-500`, tracks });
  }
  return out;
}
async function mbMatch(artistGuess, uploadTitle, ytTitles) {
  const { artist, album } = splitUpload(uploadTitle, artistGuess);
  const cands = [], errors = [];
  for (const [name, fn] of [['Tidal', () => srcTidal(artist, album)], ['Deezer', () => srcDeezer(artist, album)], ['MusicBrainz', () => srcMusicBrainz(artist, album, ytTitles.length)]]) {
    try { for (const c of await fn()) cands.push(scoreCandidate(c, ytTitles, artist)); }
    catch (e) { errors.push(`${name}: ${e.message}`); console.error('match', name, e.message); }
    const best = cands.reduce((x, y) => (!x || y.trackScore > x.trackScore ? y : x), null);
    if (best && best.trackScore >= 0.9) break; // good enough, skip slower sources
  }
  cands.sort((a, b) => b.trackScore - a.trackScore);
  const top = cands[0];
  const okMap = top ? top.map.filter(Boolean) : [];
  const confident = !!top && top.trackScore >= 0.8 && Math.abs(top.total - ytTitles.length) <= 1 && new Set(okMap.map((m) => m.n)).size === okMap.length;
  return { artist, album, confident, candidates: cands.slice(0, 6), cleaned: ytTitles.map((t) => cleanTitle(t, artist)), error: !cands.length && errors.length ? errors.join('; ') : undefined };
}

// Every 15 s: move songs MeTube has finished into Music/Artist/Album and ask Jellyfin to rescan.
async function ytSweep() {
  const pending = [...ytJobs.values()].filter((j) => j.state === 'queued');
  if (!pending.length) return;
  let hist;
  try { hist = await (await fetch(`${METUBE_URL}/history`)).json(); } catch { return; }
  const all = [...(hist.done || []), ...(hist.queue || []), ...(hist.pending || [])];
  let moved = 0;
  for (const job of pending) {
    const it = all.find((x) => (x.url || '').includes(job.id) || x.id === job.id);
    if (!it) { if (Date.now() - job.at > 6 * 3600e3) { job.state = 'error'; job.error = 'MeTube lost the download'; } continue; }
    if (it.status === 'error') { job.state = 'error'; job.error = it.msg || 'MeTube error'; continue; }
    if (it.status !== 'finished' || !it.filename) continue;
    try {
      const src = path.join(METUBE_DIR, it.filename);
      const ext = path.extname(it.filename) || '.m4a';
      const dir = path.join(MUSIC_DIR, safeName((job.albumArtist || job.artist).split(',')[0]), safeName(job.album || 'YouTube Music'));
      fs.mkdirSync(dir, { recursive: true });
      const dest = path.join(dir, (job.number ? String(job.number).padStart(2, '0') + ' ' : '') + safeName(job.title) + ext);
      try { fs.renameSync(src, dest); } catch { fs.copyFileSync(src, dest); fs.unlinkSync(src); } // different disks/shares
      await tagFile(dest, job);
      job.state = 'done'; job.path = dest; moved++;
    } catch (e) { job.state = 'error'; job.error = 'Could not move file: ' + e.message; }
  }
  saveYtJobs();
  if (moved) jfScanSoon();
}
setInterval(() => { if (settings.youtube) ytSweep().catch((e) => console.error('youtube sweep:', e.message)); }, 15000);

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

// ---------- collab songs: file them under the artist you already have ----------
const RELOC_FILE = path.join(DATA_DIR, 'collab-moves.json');
let relocs = {}; try { relocs = JSON.parse(fs.readFileSync(RELOC_FILE, 'utf8')); } catch {}
const saveRelocs = () => { try { fs.mkdirSync(DATA_DIR, { recursive: true }); fs.writeFileSync(RELOC_FILE, JSON.stringify(relocs)); } catch {} };
function localPath(jfPath) { // Jellyfin's path -> the same file as Tidefin sees it under MUSIC_DIR
  const parts = String(jfPath || '').split(/[\\/]/).filter(Boolean);
  for (let k = 0; k < parts.length; k++) { const p = path.join(MUSIC_DIR, ...parts.slice(k)); if (fs.existsSync(p)) return p; }
  return null;
}
const rmIfEmpty = (d) => { try { if (d.startsWith(MUSIC_DIR) && d !== MUSIC_DIR && !fs.readdirSync(d).filter((f) => !/^(cover|folder)\.(jpe?g|png)$/i.test(f)).length) { fs.rmSync(d, { recursive: true }); return true; } } catch {} return false; };
async function relocate(id) {
  const r = relocs[id]; if (!r) return true;
  const j = await jf(`/Items?IncludeItemTypes=Audio&Recursive=true&SearchTerm=${encodeURIComponent(r.title)}&Fields=Path,DateCreated&SortBy=DateCreated&SortOrder=Descending&Limit=40`);
  const names = r.artist.split(/,\s*/).map(snorm);
  const it = (j.Items || []).find((i) => snorm(i.Name) === snorm(r.title) && (i.Artists || []).some((a) => names.includes(snorm(a))) && snorm(i.AlbumArtist) !== snorm(r.fileUnder));
  if (!it) return false;
  const mv = await moveTrack(it, r.fileUnder);
  if (mv === null) return false;
  delete relocs[id]; saveRelocs(); jfScanSoon();
  return true;
}
// Retag a song's album artist and move it into that artist's folder. null = try again later, string = error
async function moveTrack(it, fileUnder) {
  const src = localPath(it.Path); if (!src) return null;
  if (!(await ensureFfmpeg())) return null;
  const albumDir = path.dirname(src), artistDir = path.dirname(albumDir);
  const destDir = path.join(MUSIC_DIR, safeName(fileUnder), path.basename(albumDir));
  fs.mkdirSync(destDir, { recursive: true });
  const dest = path.join(destDir, path.basename(src));
  if (dest === src) return 'already there';
  try {
    await run('ffmpeg', ['-y', '-loglevel', 'error', '-i', src, '-map', '0', '-c', 'copy', '-metadata', `album_artist=${fileUnder}`, dest]);
    for (const f of fs.readdirSync(albumDir)) if (/^(cover|folder)\.(jpe?g|png)$/i.test(f) && !fs.existsSync(path.join(destDir, f))) fs.copyFileSync(path.join(albumDir, f), path.join(destDir, f));
    fs.unlinkSync(src);
    if (rmIfEmpty(albumDir)) rmIfEmpty(artistDir);
    console.log('collab: filed', it.Name, 'under', fileUnder);
    return '';
  } catch (e) { console.error('collab move failed:', e.message); try { fs.unlinkSync(dest); } catch {} return e.message; }
}
// After each scan: songs with one artist you own are moved quietly; songs with several wait for you to pick
const TIDY_FILE = path.join(DATA_DIR, 'tidy.json');
let tidy = { pending: [], log: [] }; try { tidy = { pending: [], log: [], ...JSON.parse(fs.readFileSync(TIDY_FILE, 'utf8')) }; } catch {}
const saveTidy = () => { try { fs.mkdirSync(DATA_DIR, { recursive: true }); fs.writeFileSync(TIDY_FILE, JSON.stringify(tidy)); } catch {} };
let tidyBusy = false;
async function autoTidy() {
  if (tidyBusy) return; tidyBusy = true;
  try {
    const songs = await findCollabs();
    let moved = 0;
    const stillPending = [];
    for (const t of songs) {
      if (t.options.length === 1) {
        const j = await jf(`/Items?Ids=${t.id}&Fields=Path`); const it = (j.Items || [])[0];
        if (it && (await moveTrack(it, t.options[0].name)) === '') { moved++; tidy.log.unshift({ title: t.title, from: t.from, to: t.options[0].name, at: Date.now() }); }
      } else stillPending.push(t);
    }
    tidy.pending = stillPending; tidy.log = tidy.log.slice(0, 20); saveTidy();
    if (moved) { libIndexCache = null; jfScanSoon(); }
  } finally { tidyBusy = false; }
}
setTimeout(() => autoTidy().catch(() => {}), 60000); // once after start

const missingJob = { running: false, at: 0, done: 0, total: 0, artists: [] };
async function buildMissing() {
  missingJob.running = true; missingJob.done = 0; missingJob.artists = [];
  try {
    const ix = await getLibIndex(true);
    const counts = new Map(); for (const [a, , n] of ix.albums) if (a && n >= 3) counts.set(a, (counts.get(a) || 0) + 1);
    const names = [...counts.keys()].sort((a, b) => a.localeCompare(b));
    missingJob.total = names.length;
    for (const n of names) {
      try {
        let c = artistAlbumCache.get(n.toLowerCase());
        if (!c || Date.now() - c.at > 6 * 3600e3) { c = { at: Date.now(), data: await tidalArtistAlbums(n) }; artistAlbumCache.set(n.toLowerCase(), c); }
        if (c.data.artist && c.data.albums.length) missingJob.artists.push({ name: n, albums: c.data.albums });
      } catch (e) { console.error('missing:', n, e.message); }
      missingJob.done++;
    }
    missingJob.at = Date.now();
  } finally { missingJob.running = false; }
}

// Artists that only exist because of a collab song, and where their songs could go instead
async function findCollabs() {
  const tr = await jf('/Items?IncludeItemTypes=Audio&Recursive=true&Fields=Path,AlbumArtist&Limit=50000');
  const items = tr.Items || [];
  const byAA = new Map(); // album artist -> {name, songs, albums:Set}
  for (const t of items) {
    const aa = t.AlbumArtist || ''; if (!aa) continue;
    const k = snorm(aa); const v = byAA.get(k) || { name: aa, songs: 0, albums: new Map() };
    v.songs++; v.albums.set(t.AlbumId, (v.albums.get(t.AlbumId) || 0) + 1); byAA.set(k, v);
  }
  const isReal = (v) => v && (v.songs > 2 || [...v.albums.values()].some((n) => n >= 3));
  const out = [];
  for (const t of items) {
    const me = byAA.get(snorm(t.AlbumArtist)); if (!me || isReal(me)) continue;
    const names = new Set([...(t.Artists || []), ...String(t.AlbumArtist || '').split(/\s*(?:,|&|;|\/|\bfeat\.?|\bft\.?|\bwith\b|\bx\b)\s*/i),
      ...((String(t.Name || '').match(/\((?:feat\.?|ft\.?|with)\s+([^)]+)\)/i) || [, ''])[1].split(/\s*(?:,|&)\s*/))].map((x) => x.trim()).filter(Boolean));
    const opts = [...names].map((n) => byAA.get(snorm(n))).filter((v) => isReal(v) && snorm(v.name) !== snorm(t.AlbumArtist))
      .map((v) => ({ name: v.name, songs: v.songs })).sort((a, b) => b.songs - a.songs);
    if (opts.length) out.push({ id: t.Id, title: t.Name, from: t.AlbumArtist, album: t.Album || '', options: opts });
  }
  return out;
}
function relocateLater(id) { // Jellyfin needs a moment to see the new file
  let n = 0; const delays = [15000, 30000, 60000, 120000, 300000];
  const tick = async () => { let ok = false; try { ok = await relocate(id); } catch (e) { console.error('collab:', e.message); } if (!ok && n < delays.length) setTimeout(tick, delays[n++]); else if (!ok) { delete relocs[id]; saveRelocs(); } };
  setTimeout(tick, delays[n++]);
}

// Watch Tidarr: as soon as a download finishes, ask Jellyfin to scan, so it shows up in seconds.
const tdSeen = new Map(); let tdBusyUntil = 0, scanTimer = null, libVersion = 0;
function jfScanSoon() {
  clearTimeout(scanTimer);
  scanTimer = setTimeout(async () => {
    try { await jf('/Library/Refresh', { method: 'POST' }); } catch (e) { return console.error('scan:', e.message); }
    // Jellyfin scans in the background; drop our cache a few times while it works
    for (const ms of [4000, 10000, 20000, 40000]) setTimeout(() => { libIndexCache = null; libVersion++; }, ms);
    setTimeout(() => autoTidy().catch((e) => console.error('auto-tidy:', e.message)), 45000);
  }, 2500); // small wait so several songs finishing together share one scan
}
async function watchTidarr() {
  try {
    const items = await tidarrQueue();
    let busy = false;
    for (const it of items) {
      const prev = tdSeen.get(it.id);
      if (it.status === 'finished' && prev && prev !== 'finished') { jfScanSoon(); if (relocs[it.id]) relocateLater(it.id); }
      if (it.status !== 'finished' && it.status !== 'error') busy = true;
      tdSeen.set(it.id, it.status);
    }
    if (busy) tdBusyUntil = Date.now() + 60000;
  } catch {}
  setTimeout(watchTidarr, Date.now() < tdBusyUntil ? 3000 : 20000);
}
setTimeout(watchTidarr, 3000);

// ---------- Cast to TV: the Tidefin TV app checks in here and takes commands from your phone/PC ----------
const castDevices = new Map(); // id -> {name, seen, state, cmds, ip}
const TV_FILE = path.join(DATA_DIR, 'lg-tv.json');
let tvInfo = {}; try { tvInfo = JSON.parse(fs.readFileSync(TV_FILE, 'utf8')); } catch {}
const saveTv = () => { try { fs.mkdirSync(DATA_DIR, { recursive: true }); fs.writeFileSync(TV_FILE, JSON.stringify(tvInfo)); } catch {} };
const clientIp = (req) => String(req.socket.remoteAddress || '').replace(/^::ffff:/, '');

// Minimal WebSocket client (LG's control socket uses a self-signed certificate, so no ws library needed)
function lgSocket(ip) {
  const net = require('net'), tls = require('tls'), crypto = require('crypto');
  const tryPort = (secure) => new Promise((resolve, reject) => {
    const port = secure ? 3001 : 3000;
    const sock = secure ? tls.connect({ host: ip, port, rejectUnauthorized: false }) : net.connect({ host: ip, port });
    const key = crypto.randomBytes(16).toString('base64');
    const timer = setTimeout(() => { sock.destroy(); reject(new Error('TV did not answer')); }, 5000);
    sock.once('error', (e) => { clearTimeout(timer); reject(e); });
    sock.once(secure ? 'secureConnect' : 'connect', () => sock.write(`GET / HTTP/1.1\r\nHost: ${ip}:${port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: ${key}\r\nSec-WebSocket-Version: 13\r\n\r\n`));
    let buf = Buffer.alloc(0), open = false;
    const listeners = [];
    sock.on('data', (d) => {
      buf = Buffer.concat([buf, d]);
      if (!open) {
        const i = buf.indexOf('\r\n\r\n'); if (i < 0) return;
        if (!/^HTTP\/1\.1 101/.test(buf.slice(0, i).toString())) { clearTimeout(timer); sock.destroy(); return reject(new Error('TV refused the connection')); }
        open = true; buf = buf.slice(i + 4); clearTimeout(timer);
        resolve({
          send: (obj) => { // masked text frame
            const data = Buffer.from(JSON.stringify(obj)); const mask = crypto.randomBytes(4);
            const len = data.length; const head = len < 126 ? Buffer.from([0x81, 0x80 | len]) : len < 65536 ? Buffer.from([0x81, 0xfe, len >> 8, len & 255]) : null;
            if (!head) return;
            const out = Buffer.alloc(len); for (let k = 0; k < len; k++) out[k] = data[k] ^ mask[k & 3];
            sock.write(Buffer.concat([head, mask, out]));
          },
          on: (fn) => listeners.push(fn),
          close: () => sock.destroy(),
        });
      }
      while (open && buf.length >= 2) { // unmasked frames from the TV
        let len = buf[1] & 127, off = 2;
        if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
        else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
        if (buf.length < off + len) return;
        const op = buf[0] & 15, payload = buf.slice(off, off + len); buf = buf.slice(off + len);
        if (op === 1) { let m; try { m = JSON.parse(payload.toString()); } catch { continue; } listeners.forEach((fn) => fn(m)); }
        if (op === 8) sock.destroy();
      }
    });
  });
  return tryPort(true).catch(() => tryPort(false));
}
async function lgCommand(ip, uri, payload) {
  const ws = await lgSocket(ip);
  try {
    return await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('No answer from the TV. If a pairing prompt is showing, accept it and try again.')), 60000);
      const pairing = JSON.parse(fs.readFileSync(path.join(__dirname, 'lg-pairing.json'), 'utf8'));
      if (tvInfo.clientKey) pairing['client-key'] = tvInfo.clientKey;
      ws.on((m) => {
        if (m.id === 'reg' && m.type === 'registered') {
          if (m.payload && m.payload['client-key'] && m.payload['client-key'] !== tvInfo.clientKey) { tvInfo.clientKey = m.payload['client-key']; saveTv(); }
          ws.send({ type: 'request', id: 'mac', uri: 'ssap://com.webos.service.connectionmanager/getinfo' });
          ws.send({ type: 'request', id: 'cmd', uri, payload: payload || {} });
        } else if (m.id === 'reg' && m.type === 'error') { clearTimeout(timer); reject(new Error('TV said no: ' + (m.error || 'pairing refused'))); }
        else if (m.id === 'mac' && m.payload) {
          const mac = m.payload.wiredInfo?.macAddress || m.payload.wifiInfo?.macAddress;
          if (mac && mac !== tvInfo.mac) { tvInfo.mac = mac; saveTv(); }
        } else if (m.id === 'cmd') { clearTimeout(timer); m.type === 'error' ? reject(new Error(m.error || 'TV error')) : resolve(m.payload); }
      });
      ws.send({ type: 'register', id: 'reg', payload: pairing });
    });
  } finally { setTimeout(() => ws.close(), 1500); }
}
function wakeOnLan(mac) {
  const dgram = require('dgram');
  const hex = String(mac || '').replace(/[^0-9a-f]/gi, ''); if (hex.length !== 12) return false;
  const m = Buffer.from(hex, 'hex'); const pkt = Buffer.concat([Buffer.alloc(6, 0xff), ...Array(16).fill(m)]);
  const sock = dgram.createSocket('udp4');
  sock.bind(() => { sock.setBroadcast(true); for (const t of ['255.255.255.255', tvInfo.ip && tvInfo.ip.replace(/\.\d+$/, '.255')].filter(Boolean)) sock.send(pkt, 9, t); setTimeout(() => sock.close(), 500); });
  return true;
}
const TV_APP_ID = 'no.techfix.tidefin';
async function wakeTv() {
  if (!tvInfo.ip) throw new Error('Open Tidefin on the TV once, so Tidefin learns where the TV is.');
  let reachable = await lgSocket(tvInfo.ip).then((w) => { w.close(); return true; }).catch(() => false);
  if (!reachable) {
    if (!wakeOnLan(tvInfo.mac)) throw new Error('The TV is off, and its network address (MAC) isn\'t known yet. Turn it on by hand this time.');
    for (let i = 0; i < 12 && !reachable; i++) { await new Promise((r) => setTimeout(r, 2500)); reachable = await lgSocket(tvInfo.ip).then((w) => { w.close(); return true; }).catch(() => false); }
    if (!reachable) throw new Error('The TV didn\'t wake up. On the TV, turn on Settings → General → Devices → "Turn on via Wi-Fi" (or "Mobile TV On").');
  }
  await lgCommand(tvInfo.ip, 'ssap://system.launcher/launch', { id: TV_APP_ID });
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
      const j = await jf(`/Items?IncludeItemTypes=MusicAlbum&Recursive=true&${sort}&StartIndex=${start}&Limit=60&${jfFields}${await looseExclude()}`);
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
      return send(res, 200, { ...mapJfAlbum(a), artistId: (a.AlbumArtists && a.AlbumArtists[0] && a.AlbumArtists[0].Id) || null, tracks: (tr.Items || []).map(mapJfTrack) });
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
    if (p === '/api/yt/search') {
      if (!settings.youtube) return send(res, 200, { off: true, tracks: [] });
      const term = (q.get('q') || '').trim();
      if (!term) return send(res, 200, { tracks: [], albums: [] });
      const [tracks, official, uploads] = await Promise.all([ytSearch(term), ytSearchAlbums(term).catch(() => []), ytSearchUploads(term).catch((e) => { console.error('yt uploads:', e.message); return []; })]);
      const albums = official.concat(uploads);
      return send(res, 200, { tracks, albums });
    }
    if ((m = /^\/api\/yt\/album\/([\w-]+)$/.exec(p))) {
      if (!settings.youtube) return fail(res, 400, 'YouTube Music is turned off in Settings');
      return send(res, 200, await ytAlbum(m[1]));
    }
    if (p === '/api/yt/download' && req.method === 'POST') {
      if (!settings.youtube) return fail(res, 400, 'YouTube Music is turned off in Settings');
      const b = await readBody(req);
      const items = Array.isArray(b.items) ? b.items : [b];
      let n = 0;
      for (const it of items.slice(0, 200)) {
        const id = String(it.id || '').replace(/[^\w-]/g, '');
        if (!id) continue;
        await ytDownload({ id, title: it.title || id, artist: it.artist || 'Unknown artist', albumArtist: it.albumArtist || '', album: it.album || '', number: it.number || null, total: it.total || null, year: it.year || null, cover: /^https:\/\//.test(it.cover || '') ? it.cover : null, coverFallback: /^https:\/\//.test(it.coverFallback || '') ? it.coverFallback : null });
        n++;
      }
      if (!n) return fail(res, 400, 'Missing video id');
      if (b.single && n === 1) { const it = items[0]; addSingle(it.artist, it.title); libIndexCache = null; }
      return send(res, 200, { ok: true, queued: n });
    }
    if (p === '/api/yt/match' && req.method === 'POST') {
      const b = await readBody(req);
      const titles = (Array.isArray(b.titles) ? b.titles : []).slice(0, 100).map(String);
      if (!b.album || !titles.length) return fail(res, 400, 'Missing album or songs');
      try { return send(res, 200, await mbMatch(String(b.artist || ''), String(b.album), titles)); }
      catch (e) { return send(res, 200, { album: cleanAlbum(b.album, b.artist), confident: false, candidates: [], cleaned: titles.map((t) => cleanTitle(t, b.artist)), error: e.message }); }
    }
    if (p === '/api/yt/clear' && req.method === 'POST') {
      for (const [k, v] of ytJobs) if (v.state !== 'queued') ytJobs.delete(k);
      saveYtJobs(); return send(res, 200, { ok: true });
    }
    if (p === '/api/cast/poll' && req.method === 'POST') { // the TV app checks in every second
      const b = await readBody(req);
      const id = String(b.id || '').slice(0, 40); if (!id) return fail(res, 400, 'Missing id');
      const d = castDevices.get(id) || { cmds: [] };
      Object.assign(d, { name: String(b.name || 'LG TV').slice(0, 40), seen: Date.now(), state: b.state || null, ip: clientIp(req) });
      castDevices.set(id, d);
      if (b.tv && d.ip && d.ip !== tvInfo.ip) { tvInfo.ip = d.ip; saveTv(); }
      const cmds = d.cmds; d.cmds = [];
      return send(res, 200, { cmds });
    }
    if (p === '/api/cast/devices') {
      const now = Date.now();
      for (const [k, d] of castDevices) if (now - d.seen > 6 * 3600e3) castDevices.delete(k);
      const list = [...castDevices].map(([id, d]) => ({ id, name: d.name, online: now - d.seen < 5000, state: d.state }));
      return send(res, 200, { devices: list, tv: { known: !!tvInfo.ip, canWake: !!tvInfo.ip } });
    }
    if (p === '/api/cast/send' && req.method === 'POST') {
      const b = await readBody(req);
      const d = castDevices.get(String(b.to || ''));
      if (!d) return fail(res, 404, 'TV not found');
      d.cmds.push({ cmd: String(b.cmd || ''), data: b.data || null, at: Date.now() });
      if (d.cmds.length > 30) d.cmds = d.cmds.slice(-30);
      return send(res, 200, { ok: true });
    }
    if (p === '/api/cast/wake' && req.method === 'POST') {
      try { await wakeTv(); return send(res, 200, { ok: true }); } catch (e) { return fail(res, 502, e.message); }
    }
    if (p === '/api/library/missing') { // Tidal releases by artists you have, built in the background one artist at a time
      if (!missingJob.running && (!missingJob.at || Date.now() - missingJob.at > 6 * 3600e3 || q.get('fresh'))) buildMissing();
      return send(res, 200, { running: missingJob.running, done: missingJob.done, total: missingJob.total, artists: missingJob.artists });
    }
    if (p === '/api/tidy/status') return send(res, 200, { pending: tidy.pending, log: tidy.log });
    if (p === '/api/tidy/collabs') return send(res, 200, { songs: await findCollabs() });
    if (p === '/api/tidy/move' && req.method === 'POST') {
      const b = await readBody(req);
      const moves = (Array.isArray(b.moves) ? b.moves : []).slice(0, 500);
      let ok = 0; const failed = [];
      for (const mvq of moves) {
        const id = String(mvq.id || '').replace(/[^\w-]/g, ''); const to = String(mvq.fileUnder || '').trim(); if (!id || !to) continue;
        const j = await jf(`/Items?Ids=${id}&Fields=Path`); const it = (j.Items || [])[0];
        if (!it) { failed.push(id); continue; }
        const r = await moveTrack(it, to);
        if (r === '' || r === 'already there') ok++; else failed.push(`${it.Name}: ${r || 'file not found'}`);
      }
      const done = new Set(moves.map((x) => String(x.id)));
      tidy.pending = tidy.pending.filter((t) => !done.has(String(t.id)));
      for (const x of moves) tidy.log.unshift({ title: x.title || 'Song', to: x.fileUnder, at: Date.now() });
      tidy.log = tidy.log.slice(0, 20); saveTidy();
      libIndexCache = null; jfScanSoon();
      return send(res, 200, { moved: ok, failed });
    }
    if (p === '/api/yt/status') {
      return send(res, 200, { jobs: Object.fromEntries([...ytJobs].map(([k, v]) => [k, { state: v.state, error: v.error || null, title: v.title || '', artist: v.albumArtist || v.artist || '', album: v.album || '', at: v.at || 0 }])) });
    }
    if (p === '/api/settings') {
      if (req.method === 'POST') return send(res, 200, saveSettings(await readBody(req)));
      return send(res, 200, settings);
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
      const cutoff = new Date(Date.now() - (settings.lookbackMonths || 12) * 30.5 * 864e5).toISOString().slice(0, 10);
      const types = settings.releaseTypes || ['studio'];
      const out = [];
      for (const n of names) { // one artist at a time: Tidal rate-limits bursts
        try {
          let c = artistAlbumCache.get(n.toLowerCase());
          if (!c || Date.now() - c.at > 6 * 3600e3) { c = { at: Date.now(), data: await tidalArtistAlbums(n) }; artistAlbumCache.set(n.toLowerCase(), c); }
          for (const al of c.data.albums || []) {
            if ((al.date || '') < cutoff || !releaseAllowed(al, types)) continue;
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
      const ix = await getLibIndex(q.get('fresh'));
      return send(res, 200, { albums: ix.albums, tracks: ix.tracks });
    }
    if (p === '/api/library/singles') {
      const ix = await getLibIndex(q.get('fresh'));
      if (!ix.loose.length) return send(res, 200, { tracks: [] });
      const j = await jf(`/Items?IncludeItemTypes=Audio&Recursive=true&AlbumIds=${ix.loose.join(',')}&SortBy=DateCreated,SortName&SortOrder=Descending&Limit=2000`);
      return send(res, 200, { tracks: (j.Items || []).map(mapJfTrack) });
    }
    if (p === '/api/library/artists') {
      const start = parseInt(q.get('start') || '0', 10) || 0;
      const j = await jf(`/Artists/AlbumArtists?SortBy=SortName&SortOrder=Ascending&StartIndex=${start}&Limit=120&Fields=PrimaryImageAspectRatio&EnableImages=true`);
      // hide artists that only have 1-2 songs and no album of their own (collab leftovers; their songs are in Singles/Songs)
      const ix = await getLibIndex().catch(() => null);
      const small = ix && ix.smallArtists ? ix.smallArtists : new Set();
      const list = (j.Items || []).filter((i) => !small.has(snorm(i.Name)));
      return send(res, 200, { total: Math.max(0, (j.TotalRecordCount || 0) - ((j.Items || []).length - list.length)), artists: list.map(mapJfArtist) });
    }
    if ((m = /^\/api\/library\/artist\/([\w-]+)$/.exec(p))) {
      const id = m[1];
      const uid = await jfUser().catch(() => null);
      const [info, albums, top] = await Promise.all([
        jf(`/Items?Ids=${id}&Fields=Overview,Genres`),
        jf(`/Items?IncludeItemTypes=MusicAlbum&Recursive=true&AlbumArtistIds=${id}&SortBy=ProductionYear,PremiereDate,SortName&SortOrder=Descending&${jfFields}${await looseExclude()}`),
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
      tdBusyUntil = Date.now() + 60000;
      if (type === 'track') { addSingle(b.artist, b.title); libIndexCache = null; }
      if (type === 'track' && b.fileUnder) { relocs[id] = { title: b.title || '', artist: b.artist || '', fileUnder: String(b.fileUnder), at: Date.now() }; saveRelocs(); }
      return send(res, 200, { ok: true });
    }
    if (p === '/api/queue') return send(res, 200, { items: await tidarrQueue(), libVersion });
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
