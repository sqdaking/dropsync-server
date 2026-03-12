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
      -- Add data column if upgrading from older schema
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
  // Store full product object in data column for complete round-trip
  const fullData = JSON.stringify(p);
  await pool.query(
    `INSERT INTO products(
       id, asin, ebay_sku, ebay_item_id, title, source_url, my_price, amazon_price,
       cost, status, quantity, has_variations, variations, image_url, category,
       condition_id, last_synced, updated_at, data
     ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,NOW(),$18)
     ON CONFLICT(id) DO UPDATE SET
       asin=EXCLUDED.asin, ebay_sku=EXCLUDED.ebay_sku,
       ebay_item_id=COALESCE(EXCLUDED.ebay_item_id, products.ebay_item_id),
       title=EXCLUDED.title, source_url=EXCLUDED.source_url, my_price=EXCLUDED.my_price,
       amazon_price=EXCLUDED.amazon_price, cost=EXCLUDED.cost, status=EXCLUDED.status,
       quantity=EXCLUDED.quantity, has_variations=EXCLUDED.has_variations,
       variations=EXCLUDED.variations, image_url=EXCLUDED.image_url, category=EXCLUDED.category,
       condition_id=EXCLUDED.condition_id, last_synced=EXCLUDED.last_synced, updated_at=NOW(),
       data=EXCLUDED.data`,
    [
      p.id, p.asin, p.ebaySku||p.ebay_sku, p.ebayListingId||p.ebayItemId||p.ebay_item_id,
      p.title, p.sourceUrl||p.source_url,
      p.myPrice||p.my_price, p.amazonPrice||p.amazon_price, p.cost,
      p.status||'pending', p.quantity||1,
      p.hasVariations||p.has_variations||false,
      p.variations ? JSON.stringify(p.variations) : null,
      p.imageUrl||p.image_url||(p.images&&p.images[0])||null, p.category,
      p.conditionId||p.condition_id||'NEW',
      p.lastSynced||p.last_synced||null,
      fullData
    ]
  );
}

async function getProducts({ status, limit = 500, offset = 0 } = {}) {
  let q = 'SELECT * FROM products';
  const params = [];
  if (status) { q += ' WHERE status=$1'; params.push(status); }
  q += ' ORDER BY created_at DESC LIMIT $' + (params.length+1) + ' OFFSET $' + (params.length+2);
  params.push(limit, offset);
  const r = await pool.query(q, params);
  return r.rows.map(dbToProduct);
}

async function getProduct(id) {
  const r = await pool.query('SELECT * FROM products WHERE id=$1', [id]);
  return r.rows[0] ? dbToProduct(r.rows[0]) : null;
}

async function updateProductSync(id, { myPrice, amazonPrice, lastSynced, status, quantity }) {
  await pool.query(
    `UPDATE products SET
       my_price=$2, amazon_price=$3, last_synced=$4, status=$5, quantity=$6, updated_at=NOW(),
       data = CASE WHEN data IS NOT NULL THEN
         jsonb_set(jsonb_set(jsonb_set(jsonb_set(data,
           '{myPrice}', to_jsonb($2::numeric)),
           '{lastSynced}', to_jsonb($4::text)),
           '{status}', to_jsonb($5::text)),
           '{quantity}', to_jsonb($6::int))
         ELSE data END
     WHERE id=$1`,
    [id, myPrice, amazonPrice, lastSynced || new Date().toISOString(), status, quantity]
  );
}

async function getProductsForSync(batchSize = 100) {
  // Get listed products with sourceUrl, oldest-synced first
  const r = await pool.query(
    `SELECT * FROM products
     WHERE status='listed' AND source_url IS NOT NULL AND source_url != ''
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
    [type, title, detail, meta.productId || null, JSON.stringify(meta)]
  );
}

async function getLogs({ limit = 200, offset = 0, type } = {}) {
  let q = 'SELECT * FROM logs';
  const params = [];
  if (type) { q += ' WHERE type=$1'; params.push(type); }
  q += ' ORDER BY created_at DESC LIMIT $' + (params.length+1) + ' OFFSET $' + (params.length+2);
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
  // If full data blob stored, use it and overlay fresh sync fields
  if (row.data) {
    const d = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;
    return {
      ...d,
      // Always use DB values for these (worker may have updated them)
      status: row.status || d.status,
      myPrice: row.my_price ? parseFloat(row.my_price) : d.myPrice,
      amazonPrice: row.amazon_price ? parseFloat(row.amazon_price) : d.amazonPrice,
      quantity: row.quantity != null ? row.quantity : d.quantity,
      lastSynced: row.last_synced || d.lastSynced,
      ebaySku: row.ebay_sku || d.ebaySku,
      ebayListingId: row.ebay_item_id || d.ebayListingId || '',
    };
  }
  // Fallback for old rows without data column
  return {
    id: row.id,
    asin: row.asin,
    ebaySku: row.ebay_sku,
    ebayListingId: row.ebay_item_id,
    title: row.title,
    sourceUrl: row.source_url,
    myPrice: row.my_price ? parseFloat(row.my_price) : null,
    amazonPrice: row.amazon_price ? parseFloat(row.amazon_price) : null,
    cost: row.cost ? parseFloat(row.cost) : null,
    status: row.status,
    quantity: row.quantity,
    hasVariations: row.has_variations,
    variations: row.variations,
    images: row.image_url ? [row.image_url] : [],
    category: row.category,
    conditionId: row.condition_id,
    lastSynced: row.last_synced,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

module.exports = {
  pool, initDB,
  getSetting, setSetting, getAllSettings,
  upsertProduct, getProducts, getProduct, updateProductSync, getProductsForSync, countProducts,
  addLog, getLogs, countLogs,
};
