// Log capture must load FIRST so it captures output from every later require.
const logtail = require('./logtail');
logtail.install();

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const db = require('./db');
const vero = require('./vero');
const veroInbox = require('./vero-inbox');
const descRefresh = require('./descrefresh');
const { startWorker, runForever, getValidToken } = require('./worker');

const app = express();
logtail.mountLogTail(app);

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
    const s = await db.getAllSettings(acctOf(req));
    // Don't expose raw tokens
    const { access_token, refresh_token, ...safe } = s;
    safe.has_token = !!access_token;
    safe.has_refresh = !!refresh_token;
    safe.token_valid = !!access_token && Date.now() < (safe.token_expiry - 60000);
    res.json(safe);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Single-key read. The UI calls GET /api/settings/worker_paused on load; only
// the all-keys route existed, so every page load logged a 404 in the console.
// Returns 200 with value:null for unknown keys — absence is a valid answer,
// not an error, and the UI treats a failed response as "unknown" anyway.
app.get('/api/settings/:key', async (req, res) => {
  try {
    const key = String(req.params.key || '');
    if (!/^[\w.\-]{1,64}$/.test(key)) return res.status(400).json({ error: 'bad key' });
    // Never hand out credentials through this route.
    if (/token|secret|password/i.test(key)) return res.status(403).json({ error: 'not readable' });
    const value = await db.getSetting(key, acctOf(req));
    res.json({ key, value: value === undefined ? null : value });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/settings', async (req, res) => {  try {
    const { key, value } = req.body;
    if (!key) return res.status(400).json({ error: 'key required' });
    await db.setSetting(key, value, acctOf(req));
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/settings/bulk', async (req, res) => {
  try {
    const settings = req.body; // { key: value, ... }
    const account = acctOf(req);
    for (const [k, v] of Object.entries(settings)) {
      await db.setSetting(k, v, account);
    }
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── MULTI-ACCOUNT ────────────────────────────────────────────────────────────
// Every products/settings/logs request carries ?account=<eBay username>
// (the frontend appends it automatically once connected). Unknown/missing →
// 'default' (pre-multi-account data). Sanitized to a safe identifier.
function acctOf(req) {
  const a = String(req.query.account || req.headers['x-ds-account'] || '').trim();
  return /^[\w.\-]{1,64}$/.test(a) ? a : 'default';
}

// One-time migration: adopt all 'default'-account rows into a real account.
// Called from the ORIGINAL account's browser via the Settings link.
app.post('/api/claim-default-account', async (req, res) => {
  try {
    const { accountId, access_token } = req.body || {};
    if (!accountId || !/^[\w.\-]{1,64}$/.test(accountId) || accountId === 'default')
      return res.status(400).json({ error: 'valid accountId required' });
    // Verify the token actually belongs to the claimed account — prevents
    // account B from hijacking account A's legacy data.
    const ebayMod = require('./ebay');
    if (typeof ebayMod.resolveAccountId === 'function' && access_token) {
      const real = await ebayMod.resolveAccountId(access_token);
      if (real && real !== 'default' && real !== accountId)
        return res.status(403).json({ error: `token belongs to ${real}, not ${accountId}` });
    }
    const claimed = await db.claimDefaultAccount(accountId);
    console.log('[multi-account] claimed default rows →', accountId, claimed);
    res.json({ success: true, claimed });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Products ──────────────────────────────────────────────────────────────────
app.get('/api/products', async (req, res) => {
  try {
    const { status, limit = 500, offset = 0 } = req.query;
    const account = acctOf(req);
    const products = await db.getProducts({ status, limit: parseInt(limit), offset: parseInt(offset), account });
    const total = await db.countProducts(status, account);
    res.json({ products, total, limit: parseInt(limit), offset: parseInt(offset) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/products', async (req, res) => {
  try {
    const product = req.body;
    if (!product.id) return res.status(400).json({ error: 'product.id required' });
    await db.upsertProduct(product, acctOf(req));
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/products/bulk', async (req, res) => {
  try {
    const { products } = req.body;
    if (!Array.isArray(products)) return res.status(400).json({ error: 'products array required' });
    let saved = 0;
    const account = acctOf(req);
    for (const p of products) {
      if (p.id) { await db.upsertProduct(p, account); saved++; }
    }
    res.json({ success: true, saved });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/products/:id', async (req, res) => {
  try {
    await db.upsertProduct({ ...req.body, id: req.params.id }, acctOf(req));
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

    // Apply any inline edits from the modal (markup, quantity, etc.) BEFORE syncing.
    // Drop sync_pending — we're handling it inline, not via the worker queue.
    const merged = { ...current, ...req.body, id };
    delete merged.sync_pending;
    await db.upsertProduct(merged);

    // Get a valid eBay token (refreshes if expired)
    const { reviseProduct, getValidToken } = require('./worker');
    const token = await getValidToken();
    if (!token) {
      return res.status(401).json({ error: 'No valid eBay token — re-authenticate in Settings' });
    }

    // Pull global settings the same way the worker does
    const markupRaw    = await db.getSetting('markup');
    const globalMarkup = markupRaw != null ? parseFloat(markupRaw) : 0;
    const handlingCost = parseFloat(await db.getSetting('handlingCost') || 2);
    const webhookUrl   = await db.getSetting('webhookUrl') || null;
    const effMarkup    = (merged.markup != null && merged.markup >= 0) ? merged.markup : globalMarkup;

    // Bypass the 6h cooldown — manual sync is an explicit user action, run NOW.
    // Only this single call ignores the cooldown; worker-driven syncs still respect it.
    const productForSync = { ...merged, pushedAt: null, lastSyncedAt: null, lastSynced: null };

    const result = await reviseProduct(productForSync, token, effMarkup, handlingCost, webhookUrl);

    if (result.status === 'ok') {
      return res.json({
        success: true,
        status: result.status,
        wentOos: result.wentOos,
        priceChanges: result.priceChanges,
        stockChanges: result.stockChanges,
      });
    }
    const isErr = result.status === 'error' || result.status === 'revise_failed';
    return res.status(isErr ? 500 : 200).json({
      success: !isErr,
      status:  result.status,
      error:   result.error || null,
    });
  } catch(e) {
    console.error('[manual-sync] error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.delete('/api/products/:id', async (req, res) => {
  try {
    await db.pool.query('DELETE FROM products WHERE id=$1 AND account_id=$2', [req.params.id, acctOf(req)]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/products/count', async (req, res) => {
  try {
    const { status } = req.query;
    const count = await db.countProducts(status, acctOf(req));
    res.json({ count });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Logs ──────────────────────────────────────────────────────────────────────
app.get('/api/logs', async (req, res) => {
  try {
    const { limit = 200, offset = 0, type } = req.query;
    const account = acctOf(req);
    const logs = await db.getLogs({ limit: parseInt(limit), offset: parseInt(offset), type, account });
    const total = await db.countLogs(account);
    res.json({ logs, total });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/logs', async (req, res) => {
  try {
    const { type, title, detail, meta } = req.body;
    await db.addLog(type, title, detail, meta || {}, acctOf(req));
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/logs/bulk', async (req, res) => {
  try {
    const { logs } = req.body;
    if (!Array.isArray(logs)) return res.status(400).json({ error: 'logs array required' });
    const account = acctOf(req);
    for (const l of logs) {
      await db.addLog(l.type, l.title, l.detail, l.meta || {}, account);
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

// ── Amazon proxy — DISABLED (July 2026) ─────────────────────────────────────
// BROWSER-ONLY POLICY: the server never fetches Amazon. This endpoint used
// Railway's datacenter IP (blocked by Amazon, and every hit degraded the IP's
// reputation further). All Amazon fetching goes through the Chrome extension
// on the user's residential IP.
app.get('/api/amazon', (req, res) => {
  res.status(410).json({
    error: 'Server-side Amazon fetching is permanently disabled (browser-only policy). Install the DropSync Amazon Bridge extension.',
  });
});

// (original proxy implementation removed)


// ── eBay API handler — all actions run in-process on Railway ────────────────
const handleEbay = require('./ebay');
app.all('/api/ebay', handleEbay);

// ── AliExpress OAuth callback — AliExpress redirects sellers here after auth
// Forwards to ebay.js handler with action=ali_callback. Separate route so the
// callback URL is clean and registered with AliExpress exactly as required.
app.get('/api/aliexpress/callback', (req, res, next) => {
  req.query.action = 'ali_callback';
  return handleEbay(req, res, next);
});

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

// ── DAILY ALIEXPRESS SYNC ─────────────────────────────────────────────────
// AliExpress prices change often. Run a cron daily at 03:00 UTC that:
// 1. Pulls all listings with _source='aliexpress' from db
// 2. Resyncs each via the AliExpress API (no scraping, no proxy)
// 3. Updates eBay listing price/qty via bulkUpdatePriceQuantity
// Self-contained — doesn't require the browser tab to be open.
const cron = require('node-cron');
let _aliSyncRunning = false;
async function runDailyAliSync() {
  if (_aliSyncRunning) { console.log('[ali_cron] previous run still in flight — skipping'); return; }
  _aliSyncRunning = true;
  console.log('[ali_cron] starting daily AliExpress sync…');
  try {
    // Get a valid eBay token (worker has the refresh logic)
    let accessToken;
    try { accessToken = await getValidToken(); }
    catch(e) { console.warn('[ali_cron] no eBay token available — aborting:', e.message); _aliSyncRunning = false; return; }
    // Fetch all listed products via the existing db API. Paginate to avoid OOM.
    const aliProducts = [];
    let offset = 0;
    const pageSize = 500;
    while (true) {
      const page = await db.getProducts({ status: 'listed', limit: pageSize, offset });
      if (!page || page.length === 0) break;
      for (const p of page) {
        const d = p.data || p;
        if (d._source === 'aliexpress' && d.aliProductId && d.ebaySku) {
          aliProducts.push(p);
        }
      }
      if (page.length < pageSize) break;
      offset += pageSize;
    }
    console.log(`[ali_cron] found ${aliProducts.length} AliExpress listings to sync`);
    if (aliProducts.length === 0) { _aliSyncRunning = false; return; }
    // Process in chunks of 25 to spread API load
    let ok = 0, failed = 0;
    for (let i = 0; i < aliProducts.length; i += 25) {
      const chunk = aliProducts.slice(i, i + 25);
      const payload = {
        access_token: accessToken,
        products: chunk.map(p => ({
          ...(p.data || p),
          ebaySku: p.data?.ebaySku || p.ebaySku,
          ebayListingId: p.data?.ebayListingId || p.ebayListingId,
          aliProductId: p.data?.aliProductId || p.aliProductId,
          _source: 'aliexpress',
        })),
        limit: 25,
      };
      // Internal call rather than HTTP
      let captured;
      const mockReq = { method: 'POST', body: payload, query: { action: 'ali_bulk_sync' }, headers: {} };
      const mockRes = { json: j => captured = j, status: s => ({ json: j => captured = { ...j, _status: s } }) };
      await handleEbay(mockReq, mockRes);
      ok += captured?.ok || 0;
      failed += captured?.failed || 0;
      if (captured?.errors?.length) console.log('[ali_cron] errors:', captured.errors.slice(0, 5));
      // Pause 2s between chunks to keep AliExpress happy
      await new Promise(r => setTimeout(r, 2000));
    }
    console.log(`[ali_cron] DAILY SYNC DONE: ${ok} ok, ${failed} failed of ${aliProducts.length} total`);
  } catch(e) {
    console.error('[ali_cron] fatal:', e.message);
  } finally {
    _aliSyncRunning = false;
  }
}
// Schedule: every day at 03:00 UTC (low traffic, before US morning checks)
cron.schedule('0 3 * * *', runDailyAliSync, { timezone: 'UTC' });
console.log('[ali_cron] daily AliExpress sync scheduled at 03:00 UTC');
// Also expose a manual trigger endpoint: GET /api/aliexpress/sync-now
app.get('/api/aliexpress/sync-now', async (req, res) => {
  if (_aliSyncRunning) return res.json({ status: 'already_running' });
  runDailyAliSync(); // fire and forget
  res.json({ status: 'started', note: 'AliExpress daily sync started in background. Watch server logs.' });
});

// ── Start ─────────────────────────────────────────────────────────────────────
async function start() {
  try {
    await db.initDB();
    console.log('[Server] DB initialized');
    // VeRO / IP risk screening — audit report + do-not-relist enforcement
    try { await vero.initVero(db.pool); vero.mountVero(app, db); }
    catch(e) { console.warn('[vero] init failed:', e.message); }
    // eBay My Messages scanner — auto-flags listings named in policy/VeRO notices
    try {
      await veroInbox.initInbox(db.pool);
      veroInbox.mountInbox(app, require('./ebay').getEbayUrls);
      // Poll every 3h using the stored token of each account (best effort —
      // the button in /vero-report uses your live token and is authoritative).
      setInterval(async () => {
        try {
          const r = await db.pool.query(
            `SELECT account_id, value FROM settings WHERE key = 'access_token'`);
          for (const row of r.rows) {
            let tok = row.value;
            try { tok = JSON.parse(tok); } catch(e) {}
            if (!tok || typeof tok !== 'string') continue;
            await veroInbox.scanMessages({
              token: tok, accountId: row.account_id,
              tradingUrl: 'https://api.ebay.com/ws/api.dll',
              days: 7, autoFlag: true, autoAddBrand: true,
            }).catch(e => console.warn('[vero-inbox] poll', row.account_id, e.message));
          }
        } catch(e) { console.warn('[vero-inbox] poll cycle:', e.message); }
      }, 3 * 3600 * 1000);
    } catch(e) { console.warn('[vero-inbox] init failed:', e.message); }
    // In-place description refresh (variant-neutral copy, no relisting)
    try {
      descRefresh.initDescRefresh(db.pool);
      descRefresh.mountDescRefresh(app, require('./ebay').getEbayUrls);
    } catch(e) { console.warn('[desc] init failed:', e.message); }
    // ── WORKER DISABLED ──────────────────────────────────────────────────────
    // Hard kill switch — worker does not start. To re-enable, uncomment the
    // startWorker() line below. Manual sync from the modal still works (calls
    // the API directly), so listings can be synced one at a time on demand.
    // startWorker();
    console.log('[Server] ⛔ Worker is DISABLED (hard kill switch in server.js). Manual sync still works.');
    app.listen(PORT, '0.0.0.0', () => console.log(`[Server] Running on port ${PORT}`));
  } catch(e) {
    console.error('[Server] Failed to start:', e.message);
    process.exit(1);
  }
}

start();
