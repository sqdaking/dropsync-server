require('dotenv').config();
const express = require('express');
const cors = require('cors');
const db = require('./db');
const { startWorker, runForever, getValidToken } = require('./worker');

const app = express();

// Explicit CORS — allow all origins for every request including preflight
const corsOptions = {
  origin: '*',
  methods: 'GET,POST,PUT,DELETE,OPTIONS',
  allowedHeaders: 'Content-Type,Authorization,Accept,Accept-Language',
  credentials: false,
};
app.use(cors(corsOptions));
app.options('*', cors(corsOptions)); // handle preflight for all routes

app.use(express.json({ limit: '10mb' }));

// ── Static file serving for frontend ─────────────────────────────────────────
app.use(express.static(__dirname)); // Serve static files from server directory

const PORT = process.env.PORT || 3000;

// ── Health check & Frontend ──────────────────────────────────────────────────
app.get('/', (req, res) => {
  // Serve the DropSync frontend HTML
  res.sendFile(__dirname + '/dropsync.html');
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'DropSync v2', time: new Date().toISOString() });
});

// ── Auth ──────────────────────────────────────────────────────────────────────
app.get('/api/auth', (req, res) => {
  const clientId = process.env.EBAY_CLIENT_ID;
  const ruName   = process.env.EBAY_RU_NAME;
  const SCOPES   = 'https://api.ebay.com/oauth/api_scope https://api.ebay.com/oauth/api_scope/sell.inventory https://api.ebay.com/oauth/api_scope/sell.account https://api.ebay.com/oauth/api_scope/sell.fulfillment';
  const url = `https://auth.ebay.com/oauth2/authorize?client_id=${clientId}&redirect_uri=${encodeURIComponent(ruName)}&response_type=code&scope=${encodeURIComponent(SCOPES)}&state=production`;
  res.json({ url });
});

app.get('/api/callback', async (req, res) => {
  const { code, error } = req.query;
  if (error) return res.send(`<script>window.opener?.postMessage({type:'ebay_auth',error:'${error}'},'*');window.close();</script>`);

  const clientId     = process.env.EBAY_CLIENT_ID;
  const clientSecret = process.env.EBAY_CLIENT_SECRET;
  const ruName       = process.env.EBAY_RU_NAME;
  const creds        = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');

  try {
    const fetch = require('node-fetch');
    const r = await fetch('https://api.ebay.com/identity/v1/oauth2/token', {
      method: 'POST',
      headers: { 'Authorization': `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `grant_type=authorization_code&code=${encodeURIComponent(code)}&redirect_uri=${encodeURIComponent(ruName)}`,
    });
    const d = await r.json();
    if (d.access_token) {
      await db.setSetting('access_token', d.access_token);
      await db.setSetting('refresh_token', d.refresh_token);
      await db.setSetting('token_expiry', String(Date.now() + (d.expires_in * 1000)));
      res.send(`<script>window.opener?.postMessage({type:'ebay_auth',success:true,access_token:'${d.access_token}',refresh_token:'${d.refresh_token}',expires_in:${d.expires_in}},'*');window.close();</script>`);
    } else {
      res.send(`<script>window.opener?.postMessage({type:'ebay_auth',error:'${d.error_description||'Token exchange failed'}'},'*');window.close();</script>`);
    }
  } catch(e) {
    res.send(`<script>window.opener?.postMessage({type:'ebay_auth',error:'${e.message}'},'*');window.close();</script>`);
  }
});

app.get('/api/token', async (req, res) => {
  const token = await getValidToken();
  if (!token) return res.status(401).json({ error: 'No valid token' });
  const expiry = await db.getSetting('token_expiry');
  res.json({ access_token: token, expires_at: expiry });
});

// ── Settings ──────────────────────────────────────────────────────────────────
app.get('/api/settings', async (req, res) => {
  try {
    const s = await db.getAllSettings();
    // Don't expose raw tokens
    const { access_token, refresh_token, ...safe } = s;
    safe.has_token = !!access_token;
    safe.has_refresh = !!refresh_token;
    safe.token_valid = !!access_token && Date.now() < (safe.token_expiry - 60000);
    res.json(safe);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/settings', async (req, res) => {
  try {
    const { key, value } = req.body;
    if (!key) return res.status(400).json({ error: 'key required' });
    await db.setSetting(key, value);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/settings/bulk', async (req, res) => {
  try {
    const settings = req.body; // { key: value, ... }
    for (const [k, v] of Object.entries(settings)) {
      await db.setSetting(k, v);
    }
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Products ──────────────────────────────────────────────────────────────────
app.get('/api/products', async (req, res) => {
  try {
    const { status, limit = 500, offset = 0 } = req.query;
    const products = await db.getProducts({ status, limit: parseInt(limit), offset: parseInt(offset) });
    const total = await db.countProducts(status);
    res.json({ products, total, limit: parseInt(limit), offset: parseInt(offset) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/products', async (req, res) => {
  try {
    const product = req.body;
    if (!product.id) return res.status(400).json({ error: 'product.id required' });
    await db.upsertProduct(product);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/products/bulk', async (req, res) => {
  try {
    const { products } = req.body;
    if (!Array.isArray(products)) return res.status(400).json({ error: 'products array required' });
    let saved = 0;
    for (const p of products) {
      if (p.id) { await db.upsertProduct(p); saved++; }
    }
    res.json({ success: true, saved });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/products/:id', async (req, res) => {
  try {
    await db.upsertProduct({ ...req.body, id: req.params.id });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Queue product for immediate worker sync (called by frontend Edit modal) ──
app.post('/api/products/:id/sync', async (req, res) => {
  try {
    const id = req.params.id;
    const r = await db.pool.query('SELECT data FROM products WHERE id=$1', [id]);
    if (!r.rows.length) return res.status(404).json({ error: 'Product not found' });
    const current = typeof r.rows[0].data === 'string' ? JSON.parse(r.rows[0].data) : r.rows[0].data;
    const updated = { ...current, ...req.body, id, sync_pending: true };
    await db.upsertProduct(updated);
    res.json({ success: true, queued: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/products/:id', async (req, res) => {
  try {
    await db.pool.query('DELETE FROM products WHERE id=$1', [req.params.id]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/products/count', async (req, res) => {
  try {
    const { status } = req.query;
    const count = await db.countProducts(status);
    res.json({ count });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Logs ──────────────────────────────────────────────────────────────────────
app.get('/api/logs', async (req, res) => {
  try {
    const { limit = 200, offset = 0, type } = req.query;
    const logs = await db.getLogs({ limit: parseInt(limit), offset: parseInt(offset), type });
    const total = await db.countLogs();
    res.json({ logs, total });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/logs', async (req, res) => {
  try {
    const { type, title, detail, meta } = req.body;
    await db.addLog(type, title, detail, meta || {});
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/logs/bulk', async (req, res) => {
  try {
    const { logs } = req.body;
    if (!Array.isArray(logs)) return res.status(400).json({ error: 'logs array required' });
    for (const l of logs) {
      await db.addLog(l.type, l.title, l.detail, l.meta || {});
    }
    res.json({ success: true, saved: logs.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Browser relay ─────────────────────────────────────────────────────────────
// The browser tab acts as a residential-IP fetch proxy for Amazon URLs that
// Railway's IP gets blocked on. ebay.js fetchPage enqueues a job, the
// browser polls /needs and claims it, fetches Amazon directly, POSTs the HTML
// back to /submit. Heartbeat tells the server whether to use relay at all.

let _relayHeartbeatTs = 0;

app.post('/api/relay/heartbeat', (req, res) => {
  _relayHeartbeatTs = Date.now();
  res.json({ ok: true, ts: _relayHeartbeatTs });
});

// Server-side helper used by ebay.js to decide whether to use relay
app.get('/api/relay/status', (req, res) => {
  const ageSec = _relayHeartbeatTs ? (Date.now() - _relayHeartbeatTs) / 1000 : 999999;
  res.json({ alive: ageSec < 60, ageSec, lastHeartbeat: _relayHeartbeatTs });
});

// Browser polls this. Returns next pending job or { empty: true }.
app.get('/api/relay/needs', async (req, res) => {
  try {
    _relayHeartbeatTs = Date.now(); // polling counts as heartbeat too
    const job = await db.claimNextRelayJob();
    if (!job) return res.json({ empty: true });
    res.json({ id: job.id, url: job.url, asin: job.asin });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Browser POSTs the fetched HTML (or error)
app.post('/api/relay/submit', async (req, res) => {
  try {
    const { id, html, error } = req.body;
    if (!id) return res.status(400).json({ error: 'id required' });
    await db.submitRelayResult(id, html, error);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Cleanup old jobs every 5 min
setInterval(() => db.cleanupOldRelayJobs().catch(() => {}), 5 * 60 * 1000);

// Expose isRelayAlive() to other modules via app.locals
app.locals.isRelayAlive = () => (Date.now() - _relayHeartbeatTs) < 60000;
app.locals.relayDb = db; // ebay.js will read this for enqueue/await


app.get('/api/worker/status', async (req, res) => {
  try {
    const lastRun     = await db.getSetting('last_sync_run');
    const lastSummary = await db.getSetting('last_sync_summary');
    const listed      = await db.countProducts('listed');
    res.json({ lastRun, lastSummary, listedProducts: listed, workerRunning: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/worker/run', async (req, res) => {
  // Manual trigger
  runForever().catch(console.error);
  res.json({ success: true, message: 'Sync started' });
});

// Old external proxy removed — now handled by ebay.js directly below

// ── eBay Orders endpoint ──────────────────────────────────────────────────────
app.get('/api/orders', async (req, res) => {
  try {
    const { getValidToken } = require('./worker');
    const token = await getValidToken();
    if (!token) return res.status(401).json({ error: 'No valid eBay token' });

    const fetch = require('node-fetch');
    const limit = req.query.limit || 50;
    const offset = req.query.offset || 0;
    // Filter: last 90 days
    const fromDate = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString();

    const r = await fetch(
      `https://api.ebay.com/sell/fulfillment/v1/order?limit=${limit}&offset=${offset}&filter=lastmodifieddate:[${fromDate}..]`,
      { headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' } }
    );
    if (!r.ok) {
      const err = await r.text();
      return res.status(r.status).json({ error: err });
    }
    const d = await r.json();
    res.json(d);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});


// ── Import migration endpoint ─────────────────────────────────────────────────
app.post('/api/migrate', async (req, res) => {
  try {
    const { products = [], logs = [], settings = {} } = req.body;
    let savedProducts = 0, savedLogs = 0;

    for (const p of products) {
      if (p.id) { await db.upsertProduct(p); savedProducts++; }
    }
    for (const l of logs) {
      await db.addLog(l.type||'import', l.title||'', l.detail||'', l.meta||{});
      savedLogs++;
    }
    for (const [k, v] of Object.entries(settings)) {
      if (!['token','refreshToken','accessToken'].includes(k)) {
        await db.setSetting(k, v);
      }
    }

    res.json({ success: true, savedProducts, savedLogs });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Backfill ebay_item_id from data blob ─────────────────────────────────────
app.post('/api/backfill-listing-ids', async (req, res) => {
  try {
    const r = await db.pool.query("SELECT id, data FROM products WHERE ebay_item_id IS NULL AND data IS NOT NULL");
    let fixed = 0;
    for (const row of r.rows) {
      const d = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;
      const id = d?.ebayListingId || d?.ebayItemId || d?.ebay_item_id;
      if (id) {
        await db.pool.query('UPDATE products SET ebay_item_id=$1 WHERE id=$2', [id, row.id]);
        fixed++;
      }
    }
    res.json({ success: true, fixed, total: r.rows.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Amazon proxy — lets the browser fetch Amazon HTML through Railway ──────
// Railway IPs are not blocked by Amazon like datacenter CDN IPs, so Amazon
// scraping succeeds. Browser calls this to bypass CORS.
app.get('/api/amazon', async (req, res) => {
  const url = (req.query.url || '').trim();
  if (!url || !url.includes('amazon.')) {
    return res.status(400).json({ error: 'Invalid Amazon URL' });
  }
  // Allow any origin so the dropsync frontend can read the response
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  const fetch = require('node-fetch');
  try {
    const r = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0',
      },
      compress: true,
      timeout: 18000,
      redirect: 'follow',
    });

    if (!r.ok) {
      return res.status(r.status).json({ error: `Amazon returned HTTP ${r.status}` });
    }

    const html = await r.text();

    // Detect bot challenge pages
    if (
      html.includes('Type the characters you see in this image') ||
      html.includes('robot check') ||
      html.includes('Enter the characters you see below') ||
      html.length < 5000
    ) {
      return res.status(429).json({ error: 'Amazon bot detection triggered — try again in a moment' });
    }

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(html);
  } catch(e) {
    console.error('[amazon-proxy] error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── eBay API handler — all actions run in-process on Railway ────────────────
const handleEbay = require('./ebay');
app.all('/api/ebay', handleEbay);

// Inject the relay handle into ebay.js so fetchPage() can route Amazon URLs
// through the browser-fetched queue when a tab is alive. Without this wire,
// fetchPage falls back to direct fetch behavior (current pre-relay behavior).
if (typeof handleEbay.setRelayHandle === 'function') {
  handleEbay.setRelayHandle({
    isAlive: () => app.locals.isRelayAlive(),
    db: {
      enqueueRelayFetch: db.enqueueRelayFetch,
      awaitRelayResult:  db.awaitRelayResult,
    },
  });
  console.log('[Server] relay handle wired into ebay.js fetchPage');
}

// ── Start ─────────────────────────────────────────────────────────────────────
async function start() {
  try {
    await db.initDB();
    console.log('[Server] DB initialized');
    startWorker();
    app.listen(PORT, '0.0.0.0', () => console.log(`[Server] Running on port ${PORT}`));
  } catch(e) {
    console.error('[Server] Failed to start:', e.message);
    process.exit(1);
  }
}

start();
