const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('railway') ? { rejectUnauthorized: false } : false,
});

async function initDB() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS products (
        id TEXT PRIMARY KEY,
        asin TEXT,
        ebay_sku TEXT,
        ebay_item_id TEXT,
        title TEXT,
        source_url TEXT,
        my_price NUMERIC(10,2),
        amazon_price NUMERIC(10,2),
        cost NUMERIC(10,2),
        status TEXT DEFAULT 'pending',
        quantity INTEGER DEFAULT 1,
        has_variations BOOLEAN DEFAULT FALSE,
        variations JSONB,
        image_url TEXT,
        category TEXT,
        condition_id TEXT DEFAULT 'NEW',
        last_synced TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        data JSONB
      );
      ALTER TABLE products ADD COLUMN IF NOT EXISTS data JSONB;
      CREATE INDEX IF NOT EXISTS products_status_idx ON products(status);
      CREATE INDEX IF NOT EXISTS products_asin_idx ON products(asin);
      CREATE INDEX IF NOT EXISTS products_last_synced_idx ON products(last_synced);
      CREATE TABLE IF NOT EXISTS logs (
        id SERIAL PRIMARY KEY,
        type TEXT,
        title TEXT,
        detail TEXT,
        product_id TEXT,
        meta JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS logs_created_idx ON logs(created_at DESC);
      CREATE INDEX IF NOT EXISTS logs_type_idx ON logs(type);
      CREATE TABLE IF NOT EXISTS relay_queue (
        id SERIAL PRIMARY KEY,
        url TEXT NOT NULL,
        asin TEXT,
        status TEXT NOT NULL DEFAULT 'pending', -- pending | claimed | done | failed
        html TEXT,
        error TEXT,
        requested_at TIMESTAMPTZ DEFAULT NOW(),
        claimed_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ
      );
      CREATE INDEX IF NOT EXISTS relay_queue_status_idx ON relay_queue(status, requested_at);
      CREATE INDEX IF NOT EXISTS relay_queue_url_idx ON relay_queue(url);
    `);
    console.log('[DB] Schema ready');
  } finally {
    client.release();
  }
}

// ── Settings ──────────────────────────────────────────────────────────────────
async function getSetting(key) {
  const r = await pool.query('SELECT value FROM settings WHERE key=$1', [key]);
  if (!r.rows[0]) return null;
  try { return JSON.parse(r.rows[0].value); } catch { return r.rows[0].value; }
}

async function setSetting(key, value) {
  const v = typeof value === 'string' ? value : JSON.stringify(value);
  await pool.query(
    `INSERT INTO settings(key,value,updated_at) VALUES($1,$2,NOW())
     ON CONFLICT(key) DO UPDATE SET value=$2, updated_at=NOW()`,
    [key, v]
  );
}

async function getAllSettings() {
  const r = await pool.query('SELECT key, value FROM settings');
  const out = {};
  for (const row of r.rows) {
    try { out[row.key] = JSON.parse(row.value); } catch { out[row.key] = row.value; }
  }
  return out;
}

// ── Products ──────────────────────────────────────────────────────────────────
async function upsertProduct(p) {
  const fullData = JSON.stringify(p);
  await pool.query(
    `INSERT INTO products(
       id, asin, ebay_sku, ebay_item_id, title, source_url, my_price, amazon_price,
       cost, status, quantity, has_variations, variations, image_url, category,
       condition_id, last_synced, updated_at, data
     ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,NOW(),$18)
     ON CONFLICT(id) DO UPDATE SET
       asin=EXCLUDED.asin, ebay_sku=EXCLUDED.ebay_sku, ebay_item_id=EXCLUDED.ebay_item_id,
       title=EXCLUDED.title, source_url=EXCLUDED.source_url, my_price=EXCLUDED.my_price,
       amazon_price=EXCLUDED.amazon_price, cost=EXCLUDED.cost, status=EXCLUDED.status,
       quantity=EXCLUDED.quantity, has_variations=EXCLUDED.has_variations,
       variations=EXCLUDED.variations, image_url=EXCLUDED.image_url, category=EXCLUDED.category,
       condition_id=EXCLUDED.condition_id, last_synced=EXCLUDED.last_synced, updated_at=NOW(),
       data=EXCLUDED.data`,
    [
      p.id, p.asin,
      p.ebaySku || p.ebay_sku,
      p.ebayListingId || p.ebayItemId || p.ebay_item_id || null,
      p.title,
      p.sourceUrl || p.source_url,
      p.myPrice || p.my_price || null,
      p.amazonPrice || p.amazon_price || null,
      p.cost || null,
      p.status || 'pending',
      p.quantity || 1,
      p.hasVariations || p.has_variations || false,
      p.variations ? JSON.stringify(p.variations) : null,
      p.imageUrl || p.image_url || (p.images && p.images[0]) || null,
      p.category || null,
      p.conditionId || p.condition_id || 'NEW',
      p.lastSynced || p.last_synced || null,
      fullData,
    ]
  );
}

async function getProducts({ status, limit = 500, offset = 0 } = {}) {
  let q = 'SELECT * FROM products';
  const params = [];
  if (status) { q += ' WHERE status=$1'; params.push(status); }
  q += ' ORDER BY created_at DESC LIMIT $' + (params.length + 1) + ' OFFSET $' + (params.length + 2);
  params.push(limit, offset);
  const r = await pool.query(q, params);
  return r.rows.map(dbToProduct);
}

async function getProduct(id) {
  const r = await pool.query('SELECT * FROM products WHERE id=$1', [id]);
  return r.rows[0] ? dbToProduct(r.rows[0]) : null;
}

// FIX: explicit ::text casts on all params to avoid PostgreSQL type-inference errors
async function updateProductSync(id, { myPrice, amazonPrice, lastSynced, status, quantity, ebayListingId }) {
  const listingId = ebayListingId ? String(ebayListingId) : null;
  await pool.query(
    `UPDATE products SET
       my_price    = $2::numeric,
       amazon_price= $3::numeric,
       last_synced = $4::timestamptz,
       status      = $5::text,
       quantity    = $6::int,
       updated_at  = NOW(),
       ebay_item_id = COALESCE($7::text, ebay_item_id),
       data = CASE WHEN data IS NOT NULL THEN
         data
           || jsonb_build_object('myPrice',       $2::numeric)
           || jsonb_build_object('lastSynced',     $4::text)
           || jsonb_build_object('status',         $5::text)
           || jsonb_build_object('quantity',       $6::int)
           || jsonb_build_object('ebayListingId',  COALESCE($7::text, data->>'ebayListingId'))
         ELSE data END
     WHERE id = $1::text`,
    [id, myPrice || 0, amazonPrice || 0, lastSynced || new Date().toISOString(),
     status || 'listed', quantity ?? 1, listingId]
  );
}

async function getProductsForSync(batchSize = 30) {
  const r = await pool.query(
    `SELECT * FROM products
     WHERE status='listed'
       AND source_url IS NOT NULL AND source_url != ''
       AND (ebay_sku NOT LIKE 'DS-RECOVERED%' AND ebay_sku NOT LIKE 'DS-EXISTING%')
     ORDER BY last_synced ASC NULLS FIRST
     LIMIT $1`,
    [batchSize]
  );
  return r.rows.map(dbToProduct);
}

async function countProducts(status) {
  const q = status
    ? 'SELECT COUNT(*) FROM products WHERE status=$1'
    : 'SELECT COUNT(*) FROM products';
  const r = await pool.query(q, status ? [status] : []);
  return parseInt(r.rows[0].count);
}

// ── Logs ──────────────────────────────────────────────────────────────────────
async function addLog(type, title, detail, meta = {}) {
  await pool.query(
    'INSERT INTO logs(type,title,detail,product_id,meta) VALUES($1,$2,$3,$4,$5)',
    [type, title, detail || '', meta.productId || null, JSON.stringify(meta)]
  );
}

async function getLogs({ limit = 200, offset = 0, type } = {}) {
  let q = 'SELECT * FROM logs';
  const params = [];
  if (type) { q += ' WHERE type=$1'; params.push(type); }
  q += ' ORDER BY created_at DESC LIMIT $' + (params.length + 1) + ' OFFSET $' + (params.length + 2);
  params.push(limit, offset);
  const r = await pool.query(q, params);
  return r.rows;
}

async function countLogs() {
  const r = await pool.query('SELECT COUNT(*) FROM logs');
  return parseInt(r.rows[0].count);
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function dbToProduct(row) {
  if (row.data) {
    const d = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;
    return {
      ...d,
      status:       row.status        || d.status,
      myPrice:      row.my_price      ? parseFloat(row.my_price)      : d.myPrice,
      amazonPrice:  row.amazon_price  ? parseFloat(row.amazon_price)  : d.amazonPrice,
      quantity:     row.quantity      != null ? row.quantity           : d.quantity,
      lastSynced:   row.last_synced   || d.lastSynced,
      ebaySku:      row.ebay_sku      || d.ebaySku,
      ebayListingId: d.ebayListingId  || row.ebay_item_id || null,
    };
  }
  return {
    id:           row.id,
    asin:         row.asin,
    ebaySku:      row.ebay_sku,
    ebayListingId: row.ebay_item_id,
    title:        row.title,
    sourceUrl:    row.source_url,
    myPrice:      row.my_price      ? parseFloat(row.my_price)      : null,
    amazonPrice:  row.amazon_price  ? parseFloat(row.amazon_price)  : null,
    cost:         row.cost          ? parseFloat(row.cost)          : null,
    status:       row.status,
    quantity:     row.quantity,
    hasVariations: row.has_variations,
    variations:   row.variations,
    images:       row.image_url ? [row.image_url] : [],
    category:     row.category,
    conditionId:  row.condition_id,
    lastSynced:   row.last_synced,
    createdAt:    row.created_at,
    updatedAt:    row.updated_at,
  };
}

// ── Relay queue ──────────────────────────────────────────────────────────────
// Browser-relayed Amazon fetches. The push handler enqueues a URL it needs
// fetched, then awaits a result row. A browser tab polls /api/relay/needs,
// claims the job, fetches Amazon from its residential IP + cookies, then
// POSTs the HTML back to /api/relay/submit which marks the job done.
//
// Lifecycle: pending -> claimed -> (done | failed)
// Stale claimed jobs (> 60s old) get returned to pending so a different
// browser session can try them.
async function enqueueRelayFetch(url, asin) {
  // Dedup: if there's already a pending/claimed job for the same URL within
  // the last 60s, return that ID instead of creating a new one. Push and
  // sync often want the same ASIN within a few seconds.
  const existing = await pool.query(
    `SELECT id FROM relay_queue
     WHERE url=$1 AND status IN ('pending','claimed')
       AND requested_at > NOW() - INTERVAL '60 seconds'
     ORDER BY id DESC LIMIT 1`, [url]);
  if (existing.rows[0]) return existing.rows[0].id;
  const r = await pool.query(
    `INSERT INTO relay_queue(url, asin, status) VALUES($1, $2, 'pending') RETURNING id`,
    [url, asin || null]);
  return r.rows[0].id;
}

async function claimNextRelayJob() {
  // Atomically grab the oldest pending job and mark it claimed. Use
  // SKIP LOCKED so multiple concurrent pollers don't get the same row.
  // Also auto-recover stale claims (claimed > 60s ago).
  await pool.query(
    `UPDATE relay_queue SET status='pending', claimed_at=NULL
     WHERE status='claimed' AND claimed_at < NOW() - INTERVAL '60 seconds'`);
  const r = await pool.query(
    `UPDATE relay_queue SET status='claimed', claimed_at=NOW()
     WHERE id = (
       SELECT id FROM relay_queue WHERE status='pending'
       ORDER BY requested_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED
     )
     RETURNING id, url, asin`);
  return r.rows[0] || null;
}

async function submitRelayResult(id, html, error) {
  await pool.query(
    `UPDATE relay_queue
     SET status=$2, html=$3, error=$4, completed_at=NOW()
     WHERE id=$1`,
    [id, error ? 'failed' : 'done', html || null, error || null]);
}

async function getRelayJob(id) {
  const r = await pool.query(`SELECT * FROM relay_queue WHERE id=$1`, [id]);
  return r.rows[0] || null;
}

// Poll the queue until the job is done/failed or timeout. Returns html or null.
// After a successful read, we immediately null out the html column so the big
// payload doesn't sit in Postgres waiting for cleanupOldRelayJobs an hour later.
// Big egress win: a 2MB HTML blob gets written once, read once, then dropped.
async function awaitRelayResult(id, timeoutMs = 30000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const job = await getRelayJob(id);
    if (!job) return null;
    if (job.status === 'done') {
      const html = job.html;
      // Fire-and-forget — don't block the caller on this cleanup write
      pool.query(`UPDATE relay_queue SET html=NULL WHERE id=$1`, [id]).catch(() => {});
      return html;
    }
    if (job.status === 'failed') return null;
    await new Promise(r => setTimeout(r, 500));
  }
  return null; // timeout — caller falls back to direct fetch
}

async function cleanupOldRelayJobs() {
  // Drop jobs older than 1 hour to keep table lean
  await pool.query(`DELETE FROM relay_queue WHERE requested_at < NOW() - INTERVAL '1 hour'`);
}

module.exports = {
  pool, initDB,
  getSetting, setSetting, getAllSettings,
  // Relay
  enqueueRelayFetch, claimNextRelayJob, submitRelayResult,
  awaitRelayResult, getRelayJob, cleanupOldRelayJobs,
  upsertProduct, getProducts, getProduct, updateProductSync,
  getProductsForSync, countProducts,
  addLog, getLogs, countLogs,
};
