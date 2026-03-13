require('dotenv').config();
const express = require('express');
const cors = require('cors');
const db = require('./db');
const { startWorker, runSync, getValidToken } = require('./worker');

const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));

const PORT = process.env.PORT || 3000;

// ── Health check ──────────────────────────────────────────────────────────────
app.get('/', (req, res) => {
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
      await db.setSetting('token_expiry', Date.now() + (d.expires_in * 1000));
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

// ── Worker / Sync status ──────────────────────────────────────────────────────
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
  runSync().catch(console.error);
  res.json({ success: true, message: 'Sync started' });
});

// ── Forward to existing eBay backend (scrape, push, etc.) ────────────────────
// These actions still use the Vercel ebay.js since we're not rewriting them
app.post('/api/ebay', async (req, res) => {
  try {
    const fetch = require('node-fetch');
    const vercelUrl = process.env.VERCEL_BACKEND_URL || 'https://dropsync-one.vercel.app';
    const r = await fetch(`${vercelUrl}/api/ebay`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req.body),
    });
    const d = await r.json();

    // If a product was pushed successfully, save it to DB
    if (d.success && req.body.action === 'push' && d.ebaySku) {
      // Save the pushed product
      const p = req.body;
      await db.upsertProduct({
        id: p.id || d.ebaySku,
        asin: p.asin,
        ebaySku: d.ebaySku,
        ebayItemId: d.ebayItemId,
        title: p.title,
        sourceUrl: p.sourceUrl,
        myPrice: p.myPrice,
        cost: p.cost,
        status: 'listed',
        quantity: p.quantity || 1,
        hasVariations: p.hasVariations || false,
        variations: p.variations,
        imageUrl: p.imageUrl,
        category: p.category,
        lastSynced: null,
      });
      await db.addLog('import', `Listed: ${p.title?.slice(0,50)}`, `eBay SKU: ${d.ebaySku}`, { productId: p.id || d.ebaySku });
    }

    res.json(d);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/ebay', async (req, res) => {
  try {
    const fetch = require('node-fetch');
    const vercelUrl = process.env.VERCEL_BACKEND_URL || 'https://dropsync-one.vercel.app';
    const qs = new URLSearchParams(req.query).toString();
    const r = await fetch(`${vercelUrl}/api/ebay?${qs}`);
    const d = await r.json();
    res.json(d);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

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
