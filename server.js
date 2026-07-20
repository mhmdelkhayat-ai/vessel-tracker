const express = require('express');
const WebSocket = require('ws');
const path = require('path');

const PORT = process.env.PORT || 3000;
const API_KEY = process.env.AISSTREAM_KEY || 'c2327c4dcb3a85de1c18148ba13c519ae64dcc33';
const SHEET_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTWkaAuL6kb0stuM94ldwa3Evegttmqz92-uZlYTqFLtkFkDYuuQNrKjT21yITle90mDTsiZkkkfo3D/pub?output=csv';
const WS_URL = 'wss://stream.aisstream.io/v0/stream';

// Route corridors: East Asia, Indian Ocean, Red Sea, Med, Atlantic, Southern Africa
const BOXES = [
  [[5, 98], [45, 140]],
  [[-15, 44], [30, 98]],
  [[10, 31], [33, 52]],
  [[30, -7], [46, 37]],
  [[5, -100], [50, -40]],
  [[0, -45], [45, 0]],
  [[-40, 10], [5, 55]]
];

let targets = [];
let stream = { connected: false, msgCount: 0, lastMessageAt: null, lastError: null, startedAt: Date.now() };

function normalize(name) {
  if (!name) return '';
  let n = String(name).toUpperCase();
  n = n.replace(/\bV\.?\s*\d+\w*/g, ' ');
  n = n.replace(/[^A-Z0-9]+/g, ' ').trim();
  n = n.replace(/\s+\d{1,4}$/, '');
  return n.replace(/\s+/g, ' ').trim();
}

function splitCsvLine(line) {
  const cells = []; let cur = '', q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (q) { if (ch === '"') { if (line[i + 1] === '"') { cur += '"'; i++; } else q = false; } else cur += ch; }
    else { if (ch === '"') q = true; else if (ch === ',') { cells.push(cur); cur = ''; } else cur += ch; }
  }
  cells.push(cur);
  return cells.map(c => c.trim());
}

function parseSheet(text) {
  const rows = [];
  for (const raw of text.split(/\r?\n/)) {
    if (!raw.trim()) continue;
    const c = splitCsvLine(raw);
    const name = (c[0] || '').trim();
    if (!name) continue;
    if (/^vessel\s*name$/i.test(name)) continue;
    if (/^total$/i.test(name)) continue;
    rows.push({ name, origin: (c[1] || '').trim(), plannedEta: (c[2] || '').trim(), qty: (c[c.length - 1] || '').trim() });
  }
  return rows;
}

async function syncSheet() {
  try {
    const r = await fetch(SHEET_URL, { cache: 'no-store' });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const rows = parseSheet(await r.text());
    const byNorm = {}; for (const t of targets) byNorm[t.norm] = t;
    const next = []; const seen = new Set();
    for (const row of rows) {
      const norm = normalize(row.name);
      if (!norm || norm.length < 3 || seen.has(norm)) continue;
      seen.add(norm);
      const prev = byNorm[norm];
      if (prev) { prev.raw = row.name; prev.origin = row.origin; prev.plannedEta = row.plannedEta; prev.qty = row.qty; next.push(prev); }
      else next.push({ raw: row.name, norm, origin: row.origin, plannedEta: row.plannedEta, qty: row.qty, adhoc: false, data: null });
    }
    for (const t of targets) if (t.adhoc && !seen.has(t.norm)) { seen.add(t.norm); next.push(t); }
    targets = next;
    console.log('[sheet] synced', targets.length, 'vessels');
  } catch (e) {
    console.error('[sheet] sync failed:', e.message);
  }
}

function matchTarget(aisName) {
  const n = normalize(aisName);
  if (!n || n.length < 3) return null;
  for (const t of targets) {
    if (t.norm === n) return t;
    if (t.norm.length >= 5 && n.length >= 5 && (n.includes(t.norm) || t.norm.includes(n))) return t;
  }
  return null;
}

let ws = null, retryDelay = 5000;

function connect() {
  ws = new WebSocket(WS_URL);
  ws.on('open', () => {
    ws.send(JSON.stringify({ APIKey: API_KEY, BoundingBoxes: BOXES, FilterMessageTypes: ['PositionReport', 'ShipStaticData'] }));
    stream.connected = true; stream.lastError = null; retryDelay = 5000;
    console.log('[ais] connected and subscribed');
  });
  ws.on('message', buf => {
    let m; try { m = JSON.parse(buf); } catch (e) { return; }
    if (m.error) {
      stream.lastError = m.error;
      console.error('[ais] error:', m.error);
      return;
    }
    stream.msgCount++; stream.lastMessageAt = Date.now();
    const meta = m.MetaData || m.Metadata;
    if (!meta || !meta.ShipName) return;
    const t = matchTarget(meta.ShipName);
    if (!t) return;
    if (!t.data) t.data = {};
    t.data.mmsi = meta.MMSI;
    t.data.aisName = String(meta.ShipName).trim();
    t.data.time = Date.now();
    if (m.MessageType === 'PositionReport' && m.Message && m.Message.PositionReport) {
      const p = m.Message.PositionReport;
      t.data.lat = p.Latitude ?? meta.latitude;
      t.data.lon = p.Longitude ?? meta.longitude;
      t.data.sog = p.Sog; t.data.cog = p.Cog;
    } else if (meta.latitude != null) {
      t.data.lat = t.data.lat ?? meta.latitude;
      t.data.lon = t.data.lon ?? meta.longitude;
    }
    if (m.MessageType === 'ShipStaticData' && m.Message && m.Message.ShipStaticData) {
      const s = m.Message.ShipStaticData;
      t.data.dest = (s.Destination || '').trim();
      if (s.Eta && s.Eta.Month > 0) t.data.aisEta = s.Eta;
    }
    console.log('[match]', t.raw, '->', t.data.aisName, t.data.lat != null ? (t.data.lat.toFixed(2) + ',' + t.data.lon.toFixed(2)) : '(static)');
  });
  ws.on('close', () => {
    stream.connected = false;
    console.log('[ais] closed, retrying in', retryDelay / 1000, 's');
    setTimeout(connect, retryDelay);
    retryDelay = Math.min(retryDelay * 2, 60000);
  });
  ws.on('error', e => {
    stream.lastError = e.message;
    console.error('[ais] socket error:', e.message);
  });
}

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/fleet', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.json({
    stream: {
      connected: stream.connected,
      msgCount: stream.msgCount,
      lastMessageAt: stream.lastMessageAt,
      lastError: stream.lastError
    },
    fleet: targets.map(t => ({
      raw: t.raw, origin: t.origin, plannedEta: t.plannedEta, qty: t.qty, adhoc: t.adhoc, data: t.data
    }))
  });
});

app.post('/api/add', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  const name = String((req.body && req.body.name) || '').trim();
  const norm = normalize(name);
  if (!norm || norm.length < 3) return res.status(400).json({ error: 'Name too short to match reliably.' });
  if (targets.some(t => t.norm === norm)) return res.status(409).json({ error: 'Already tracked.' });
  targets.push({ raw: name.toUpperCase(), norm, origin: '', plannedEta: '', qty: '', adhoc: true, data: null });
  res.json({ ok: true });
});

app.listen(PORT, async () => {
  console.log('Ford Naghi Vessel Tracker relay on port', PORT);
  await syncSheet();
  setInterval(syncSheet, 30 * 60 * 1000);
  connect();
});
