// logtail.js — in-app log capture, live tail, and Railway rate-limit protection
//
// THE PROBLEM
// Railway caps logging at 500 lines/sec per replica and DROPS everything above
// it, renders through an ingestion pipeline (seconds of lag), can show lines
// out of order, and scopes the view to one deployment + time window. smartSync
// prints ~60 lines for a 48-variant listing, so during concurrent syncs you
// both wait for logs and silently lose them.
//
// WHAT THIS DOES
//   1. Keeps the last N lines in memory — complete, ordered, instant.
//   2. Streams them to your browser over SSE at /logs-live (no Railway pipeline).
//   3. Throttles what goes to stdout so you stop hitting the 500/sec limit,
//      while the browser tail still sees EVERYTHING.
//
// LOG_LEVEL controls what reaches Railway's stdout only — never what reaches
// the live tail:
//   debug → forward everything
//   info  → (default) drop known per-variant noise, keep summaries
//   warn  → warnings and errors only
//
// Load this FIRST in server.js so it captures every later require's output.

const RING_SIZE = parseInt(process.env.LOG_RING_SIZE) || 3000;
const LEVEL = (process.env.LOG_LEVEL || 'info').toLowerCase();
const STDOUT_CAP = parseInt(process.env.LOG_STDOUT_PER_SEC) || 200; // keep under Railway's 500

// Per-variant / high-frequency lines. At LOG_LEVEL=info these stay in the live
// tail but don't get shipped to Railway — this is the bulk of the volume.
const NOISY = [
  /\[smartSync\] \S+ asin=/,          // one line per variant
  /→ ORPHAN qty=0/,
  /→ mapping corrected but no price/,
  /\[prefetchAsin\]/,
  /\[browserFetch\]/,
  /\[cache\] (hit|miss)/,
  /\[relay\] Revising \[/,
];

const ring = [];
let seq = 0;
const clients = new Set();

// stdout throttling state
let windowStart = Date.now();
let sentThisSecond = 0;
let droppedThisSecond = 0;

const orig = {
  log: console.log.bind(console),
  warn: console.warn.bind(console),
  error: console.error.bind(console),
  info: console.info ? console.info.bind(console) : console.log.bind(console),
};

function fmt(args) {
  return args.map(a => {
    if (typeof a === 'string') return a;
    if (a instanceof Error) return a.stack || a.message;
    try { return JSON.stringify(a); } catch (e) { return String(a); }
  }).join(' ');
}

function shouldForward(level, text) {
  if (level === 'error' || level === 'warn') return true;
  if (LEVEL === 'debug') return true;
  if (LEVEL === 'warn') return false;
  return !NOISY.some(re => re.test(text));   // LEVEL === 'info'
}

function push(level, text) {
  const entry = { i: ++seq, t: Date.now(), level, text };
  ring.push(entry);
  if (ring.length > RING_SIZE) ring.splice(0, ring.length - RING_SIZE);
  // fan out to live tail clients (never throttled, never filtered)
  const payload = `data: ${JSON.stringify(entry)}\n\n`;
  for (const res of clients) {
    try { res.write(payload); } catch (e) { clients.delete(res); }
  }
  return entry;
}

function throttledOut(level, text, args) {
  const now = Date.now();
  if (now - windowStart >= 1000) {
    if (droppedThisSecond > 0) {
      orig.warn(`[logtail] suppressed ${droppedThisSecond} lines from stdout in the last second (still visible at /logs-live)`);
    }
    windowStart = now; sentThisSecond = 0; droppedThisSecond = 0;
  }
  if (sentThisSecond >= STDOUT_CAP) { droppedThisSecond++; return; }
  sentThisSecond++;
  (orig[level] || orig.log)(...args);
}

function install() {
  for (const level of ['log', 'warn', 'error', 'info']) {
    console[level] = (...args) => {
      const text = fmt(args);
      push(level === 'info' ? 'log' : level, text);
      if (shouldForward(level, text)) throttledOut(level, text, args);
    };
  }
  orig.log(`[logtail] capturing to ring buffer (${RING_SIZE} lines), LOG_LEVEL=${LEVEL}, stdout cap ${STDOUT_CAP}/s — live tail at /logs-live`);
}

function mountLogTail(app) {
  // SSE stream — pushes each new line the instant it happens
  app.get('/api/logs/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders?.();
    // replay recent history so the page isn't empty on open
    const back = Math.min(parseInt(req.query.backlog) || 300, RING_SIZE);
    for (const e of ring.slice(-back)) res.write(`data: ${JSON.stringify(e)}\n\n`);
    clients.add(res);
    const ka = setInterval(() => { try { res.write(': ka\n\n'); } catch (e) {} }, 20000);
    req.on('close', () => { clearInterval(ka); clients.delete(res); });
  });

  // Plain JSON snapshot (for grep/scripting)
  app.get('/api/logs/tail', (req, res) => {
    const n = Math.min(parseInt(req.query.n) || 500, RING_SIZE);
    const q = req.query.q;
    let out = ring.slice(-n);
    if (q) { try { const re = new RegExp(q, 'i'); out = out.filter(e => re.test(e.text)); } catch (e) {} }
    res.json({ count: out.length, buffered: ring.length, lines: out });
  });

  app.get('/logs-live', (req, res) => {
    res.setHeader('Content-Type', 'text/html');
    res.send(PAGE);
  });

  console.log('[logtail] routes mounted: /logs-live, /api/logs/stream, /api/logs/tail');
}

const PAGE = `<!DOCTYPE html><html><head><meta charset="utf-8"><title>DropSync — live logs</title><style>
*{box-sizing:border-box}body{margin:0;font:13px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace;background:#0b0d12;color:#d7dbe4}
header{position:sticky;top:0;background:#11141b;border-bottom:1px solid #222736;padding:10px 14px;display:flex;gap:8px;align-items:center;flex-wrap:wrap}
input,select,button{background:#1a1e27;border:1px solid #2a3040;color:#e6e8ee;border-radius:6px;padding:6px 9px;font:inherit}
input{flex:1;min-width:180px} button{cursor:pointer} button.on{background:#2b5cd9;border-color:#2b5cd9}
#dot{width:9px;height:9px;border-radius:50%;background:#666} #dot.live{background:#6fd08c}
#out{padding:10px 14px;white-space:pre-wrap;word-break:break-word}
.l{padding:1px 0;border-bottom:1px solid #14171f}
.warn{color:#ffc36f}.error{color:#ff7a7a}.hit{background:#2b5cd933}
.ts{color:#5b6478;margin-right:8px}
</style></head><body>
<header>
  <span id="dot"></span><b>DropSync live logs</b>
  <input id="f" placeholder="filter (regex) — e.g. smartSync|PHASE|CORRECTED">
  <select id="lvl"><option value="">all levels</option><option value="warn">warn+error</option><option value="error">error only</option></select>
  <button id="pause">Pause</button><button id="clear">Clear</button>
  <button id="wrapb" class="on">Autoscroll</button>
  <span id="stat" style="color:#5b6478"></span>
</header>
<div id="out"></div>
<script>
let paused=false, autoscroll=true, total=0, shown=0;
const out=document.getElementById('out'), f=document.getElementById('f'), lvl=document.getElementById('lvl');
function matches(e){
  if(lvl.value==='error'&&e.level!=='error')return false;
  if(lvl.value==='warn'&&!(e.level==='warn'||e.level==='error'))return false;
  if(f.value){ try{ if(!new RegExp(f.value,'i').test(e.text)) return false; }catch(err){} }
  return true;
}
function render(e){
  if(paused||!matches(e))return;
  const d=document.createElement('div');
  d.className='l '+(e.level==='warn'?'warn':e.level==='error'?'error':'');
  d.innerHTML='<span class="ts">'+new Date(e.t).toLocaleTimeString()+'</span>'+
    e.text.replace(/[<>&]/g,c=>({'<':'&lt;','>':'&gt;','&':'&amp;'}[c]));
  out.appendChild(d); shown++;
  while(out.children.length>4000) out.removeChild(out.firstChild);
  if(autoscroll) window.scrollTo(0,document.body.scrollHeight);
  document.getElementById('stat').textContent=shown+' shown / '+total+' received';
}
const es=new EventSource('/api/logs/stream?backlog=400');
es.onopen=()=>document.getElementById('dot').className='live';
es.onerror=()=>document.getElementById('dot').className='';
es.onmessage=ev=>{ total++; render(JSON.parse(ev.data)); };
document.getElementById('pause').onclick=e=>{paused=!paused;e.target.classList.toggle('on',paused);e.target.textContent=paused?'Resume':'Pause';};
document.getElementById('clear').onclick=()=>{out.innerHTML='';shown=0;};
document.getElementById('wrapb').onclick=e=>{autoscroll=!autoscroll;e.target.classList.toggle('on',autoscroll);};
</script></body></html>`;

module.exports = { install, mountLogTail, ring };
