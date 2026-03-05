from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()

HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>llogr</title>
<style>
  *, *::before, *::after { box-sizing: border-box; }
  body { font-family: system-ui, -apple-system, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; color: #333; }
  h1 { margin: 0 0 20px; font-size: 1.4rem; }
  .card { background: #fff; border-radius: 8px; padding: 20px; margin-bottom: 16px; box-shadow: 0 1px 3px rgba(0,0,0,.1); }
  label { display: block; font-size: .85rem; font-weight: 600; margin-bottom: 4px; }
  input[type=text] { width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px; font-size: .9rem; }
  .row { display: flex; gap: 12px; flex-wrap: wrap; }
  .row > div { flex: 1; min-width: 140px; }
  button { padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer; font-size: .9rem; font-weight: 600; }
  .btn-primary { background: #2563eb; color: #fff; }
  .btn-primary:hover { background: #1d4ed8; }
  .btn-secondary { background: #e5e7eb; color: #333; }
  .btn-secondary:hover { background: #d1d5db; }
  table { width: 100%; border-collapse: collapse; font-size: .85rem; }
  th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid #eee; }
  th { background: #f9fafb; font-weight: 600; }
  #status { margin: 10px 0; font-size: .85rem; color: #666; }
  #urls { margin-top: 12px; }
  #urls a { display: block; word-break: break-all; margin: 4px 0; font-size: .85rem; }
  .actions { display: flex; gap: 8px; margin-top: 12px; }
  .btn-sm { padding: 4px 10px; font-size: .8rem; }
  .time-picker { position: relative; }
  .time-btn { background: #fff; border: 1px solid #ddd; border-radius: 4px; padding: 8px 12px; font-size: .85rem; cursor: pointer; min-width: 220px; text-align: left; display: flex; justify-content: space-between; align-items: center; }
  .time-btn:hover { border-color: #aaa; }
  .time-btn .arrow { font-size: .7rem; color: #999; }
  .time-dropdown { display: none; position: absolute; top: 100%; left: 0; margin-top: 4px; background: #fff; border: 1px solid #ddd; border-radius: 6px; box-shadow: 0 4px 12px rgba(0,0,0,.15); z-index: 50; min-width: 320px; }
  .time-dropdown.open { display: flex; }
  .time-presets { border-right: 1px solid #eee; padding: 8px 0; min-width: 150px; }
  .time-presets div { padding: 6px 14px; cursor: pointer; font-size: .85rem; }
  .time-presets div:hover { background: #f0f4ff; }
  .time-presets div.active { background: #e0e7ff; font-weight: 600; }
  .time-custom { padding: 12px; }
  .time-custom label { font-size: .8rem; margin-bottom: 2px; }
  .time-custom input[type=datetime-local] { width: 100%; padding: 6px; border: 1px solid #ddd; border-radius: 4px; font-size: .85rem; margin-bottom: 8px; }
  .time-custom button { width: 100%; }
  .modal-overlay { position: fixed; inset: 0; background: rgba(0,0,0,.5); display: flex; align-items: center; justify-content: center; z-index: 100; }
  .modal { background: #fff; border-radius: 8px; width: 90vw; max-width: 900px; max-height: 85vh; display: flex; flex-direction: column; box-shadow: 0 8px 30px rgba(0,0,0,.2); }
  .modal-header { display: flex; justify-content: space-between; align-items: center; padding: 12px 16px; border-bottom: 1px solid #eee; }
  .modal-header h2 { margin: 0; font-size: 1rem; word-break: break-all; }
  .modal-close { background: none; border: none; font-size: 1.4rem; cursor: pointer; padding: 0 4px; color: #666; }
  .modal-close:hover { color: #333; }
  .modal-body { overflow: auto; padding: 16px; flex: 1; }
  .modal-body pre { margin: 0; white-space: pre-wrap; word-break: break-word; font-size: .82rem; line-height: 1.5; }
</style>
</head>
<body>
<h1>llogr — log browser</h1>

<div class="card">
  <div class="row">
    <div><label>Public Key</label><input type="text" id="pk" placeholder="pk-..."></div>
    <div><label>Secret Key</label><input type="text" id="sk" placeholder="sk-..."></div>
  </div>
</div>

<div class="card">
  <div class="row" style="align-items:flex-end">
    <div>
      <label>Time Range</label>
      <div class="time-picker" id="timePicker">
        <button type="button" class="time-btn" onclick="toggleTimeDropdown()">
          <span id="timeLabel">Last 24 hours</span>
          <span class="arrow">&#9660;</span>
        </button>
        <div class="time-dropdown" id="timeDropdown">
          <div class="time-presets" id="timePresets">
            <div onclick="setPreset('15m','Last 15 minutes')">Last 15 minutes</div>
            <div onclick="setPreset('1h','Last 1 hour')">Last 1 hour</div>
            <div onclick="setPreset('4h','Last 4 hours')">Last 4 hours</div>
            <div class="active" onclick="setPreset('24h','Last 24 hours')">Last 24 hours</div>
            <div onclick="setPreset('7d','Last 7 days')">Last 7 days</div>
            <div onclick="setPreset('30d','Last 30 days')">Last 30 days</div>
            <div onclick="setPreset('all','All time')">All time</div>
          </div>
          <div class="time-custom">
            <label>From</label>
            <input type="datetime-local" id="time_from">
            <label>To</label>
            <input type="datetime-local" id="time_to">
            <button class="btn-primary btn-sm" onclick="applyCustomRange()">Apply</button>
          </div>
        </div>
      </div>
    </div>
    <div><label>Session ID</label><input type="text" id="f_session"></div>
    <div><label>Trace ID</label><input type="text" id="f_trace"></div>
    <div><label>Trace Type</label><input type="text" id="f_tracetype"></div>
    <div><label>Input Hash</label><input type="text" id="f_hash"></div>
  </div>
  <div class="actions">
    <button class="btn-primary" onclick="loadLogs()">Search</button>
  </div>
</div>

<div id="status"></div>

<div class="card" id="results" style="display:none">
  <table>
    <thead>
      <tr>
        <th><input type="checkbox" id="selectAll" onchange="toggleAll(this)"></th>
        <th>Timestamp</th>
        <th>Session ID</th>
        <th>Trace ID</th>
        <th>Trace Type</th>
        <th>Input Hash</th>
        <th></th>
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>
  <div class="actions">
    <button class="btn-primary" onclick="getUrls()">Download catalog</button>
    <button class="btn-secondary" onclick="getIndex()">Download index</button>
  </div>
  <div id="urls"></div>
</div>

<div id="modal"></div>

<script>
const BASE = '{{BASE_PATH}}';
let timePreset = '24h';
let customFrom = null, customTo = null;

const PRESETS = {
  '15m': 15*60*1000,
  '1h': 60*60*1000,
  '4h': 4*60*60*1000,
  '24h': 24*60*60*1000,
  '7d': 7*24*60*60*1000,
  '30d': 30*24*60*60*1000,
  'all': null,
};

function toggleTimeDropdown() {
  document.getElementById('timeDropdown').classList.toggle('open');
}

function setPreset(key, label) {
  timePreset = key;
  customFrom = null; customTo = null;
  document.getElementById('timeLabel').textContent = label;
  document.querySelectorAll('#timePresets div').forEach(d => d.classList.remove('active'));
  event.target.classList.add('active');
  document.getElementById('timeDropdown').classList.remove('open');
}

function applyCustomRange() {
  const from = document.getElementById('time_from').value;
  const to = document.getElementById('time_to').value;
  if (!from || !to) return;
  customFrom = new Date(from).toISOString();
  customTo = new Date(to).toISOString();
  timePreset = null;
  document.getElementById('timeLabel').textContent = from.replace('T',' ') + '  \u2192  ' + to.replace('T',' ');
  document.querySelectorAll('#timePresets div').forEach(d => d.classList.remove('active'));
  document.getElementById('timeDropdown').classList.remove('open');
}

function getTimeRange() {
  if (customFrom && customTo) return { start: customFrom, end: customTo };
  const ms = PRESETS[timePreset];
  if (!ms) return {};
  const now = new Date();
  return { start: new Date(now - ms).toISOString(), end: now.toISOString() };
}

document.addEventListener('click', function(e) {
  const picker = document.getElementById('timePicker');
  if (!picker.contains(e.target)) document.getElementById('timeDropdown').classList.remove('open');
});

function authHeader() {
  return 'Basic ' + btoa(document.getElementById('pk').value + ':' + document.getElementById('sk').value);
}

async function loadLogs() {
  const status = document.getElementById('status');
  status.textContent = 'Loading...';
  document.getElementById('urls').innerHTML = '';
  const params = new URLSearchParams();
  const range = getTimeRange();
  if (range.start) params.set('start', range.start);
  if (range.end) params.set('end', range.end);
  const s = document.getElementById('f_session').value.trim();
  const t = document.getElementById('f_trace').value.trim();
  const tt = document.getElementById('f_tracetype').value.trim();
  const h = document.getElementById('f_hash').value.trim();
  if (s) params.set('session_id', s);
  if (t) params.set('trace_id', t);
  if (tt) params.set('trace_type', tt);
  if (h) params.set('input_hash', h);
  try {
    const resp = await fetch(BASE + '/api/public/logs/list?' + params, {
      headers: { 'Authorization': authHeader() }
    });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    renderTable(data.files);
    status.textContent = data.files.length + ' batch(es) found';
  } catch (e) {
    status.textContent = 'Error: ' + e.message;
  }
}

function renderTable(files) {
  const tbody = document.getElementById('tbody');
  tbody.innerHTML = '';
  document.getElementById('results').style.display = files.length ? '' : 'none';
  files.forEach(f => {
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td><input type="checkbox" class="sel" value="' + f.key.replace(/"/g, '&quot;') + '"></td>' +
      '<td>' + esc(f.timestamp) + '</td>' +
      '<td>' + esc(f.session_id) + '</td>' +
      '<td>' + esc(f.trace_id) + '</td>' +
      '<td>' + esc(f.trace_type) + '</td>' +
      '<td>' + esc(f.input_hash) + '</td>' +
      '<td><button class="btn-secondary btn-sm" onclick="preview(this)" data-key="' + f.key.replace(/"/g, '&quot;') + '">Preview</button></td>';
    tbody.appendChild(tr);
  });
}

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

function toggleAll(master) {
  document.querySelectorAll('.sel').forEach(c => c.checked = master.checked);
}

async function getUrls() {
  const keys = [...document.querySelectorAll('.sel:checked')].map(c => c.value);
  if (!keys.length) return;
  const urlsDiv = document.getElementById('urls');
  urlsDiv.textContent = 'Fetching ' + keys.length + ' file(s)...';
  try {
    const resp = await fetch(BASE + '/api/public/logs/urls', {
      method: 'POST',
      headers: { 'Authorization': authHeader(), 'Content-Type': 'application/json' },
      body: JSON.stringify({ keys })
    });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    const all = [];
    for (const f of data.files) {
      const r = await fetch(f.url);
      if (!r.ok) throw new Error('Failed to fetch ' + f.key);
      const text = await r.text();
      const events = text.trim().split('\\n').map(line => JSON.parse(line));
      all.push(...events);
    }
    const blob = new Blob([JSON.stringify(all, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'catalog.json';
    a.click();
    URL.revokeObjectURL(url);
    urlsDiv.textContent = 'Downloaded ' + all.length + ' event(s).';
  } catch (e) {
    urlsDiv.textContent = 'Error: ' + e.message;
  }
}

async function getIndex() {
  const keys = [...document.querySelectorAll('.sel:checked')].map(c => c.value);
  if (!keys.length) return;
  const urlsDiv = document.getElementById('urls');
  urlsDiv.textContent = 'Fetching URLs for ' + keys.length + ' file(s)...';
  try {
    const resp = await fetch(BASE + '/api/public/logs/urls', {
      method: 'POST',
      headers: { 'Authorization': authHeader(), 'Content-Type': 'application/json' },
      body: JSON.stringify({ keys })
    });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    const lines = data.files.map(f => JSON.stringify({ url: f.url }));
    const blob = new Blob([lines.join('\\n') + '\\n'], { type: 'application/x-ndjson' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'index.jsonl';
    a.click();
    URL.revokeObjectURL(url);
    urlsDiv.textContent = 'Downloaded index with ' + data.files.length + ' file(s).';
  } catch (e) {
    urlsDiv.textContent = 'Error: ' + e.message;
  }
}

async function preview(btn) {
  const key = btn.dataset.key;
  const modal = document.getElementById('modal');
  modal.innerHTML =
    '<div class="modal-overlay" onclick="if(event.target===this)closeModal()">' +
    '<div class="modal"><div class="modal-header"><h2>' + esc(key) + '</h2>' +
    '<button class="modal-close" onclick="closeModal()">&times;</button></div>' +
    '<div class="modal-body"><pre>Loading...</pre></div></div></div>';
  try {
    const resp = await fetch(BASE + '/api/public/logs/urls', {
      method: 'POST',
      headers: { 'Authorization': authHeader(), 'Content-Type': 'application/json' },
      body: JSON.stringify({ keys: [key] })
    });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    if (!data.files.length) throw new Error('No URL returned');
    const content = await fetch(data.files[0].url);
    if (!content.ok) throw new Error('HTTP ' + content.status);
    const text = await content.text();
    const json = text.trim().split('\\n').map(line => JSON.parse(line));
    modal.querySelector('pre').textContent = JSON.stringify(json, null, 2);
  } catch (e) {
    modal.querySelector('pre').textContent = 'Error: ' + e.message;
  }
}

function closeModal() {
  document.getElementById('modal').innerHTML = '';
}
</script>
</body>
</html>
"""


@router.get("/", response_class=HTMLResponse)
async def ui(request: Request) -> HTMLResponse:
    base = request.scope.get("root_path", "").rstrip("/")
    return HTMLResponse(content=HTML.replace("{{BASE_PATH}}", base))
