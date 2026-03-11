const cron = require('node-cron');
const db = require('./db');

let isSyncing = false;

// ── Token management ──────────────────────────────────────────────────────────
async function getValidToken() {
  const accessToken  = await db.getSetting('access_token');
  const tokenExpiry  = await db.getSetting('token_expiry');
  const refreshToken = await db.getSetting('refresh_token');

  // Still valid
  if (accessToken && tokenExpiry && Date.now() < tokenExpiry - 60000) {
    return accessToken;
  }

  // Refresh
  if (!refreshToken) return null;

  const clientId     = process.env.EBAY_CLIENT_ID;
  const clientSecret = process.env.EBAY_CLIENT_SECRET;
  if (!clientId || !clientSecret) return null;

  const creds = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
  const SCOPES = 'https://api.ebay.com/oauth/api_scope https://api.ebay.com/oauth/api_scope/sell.inventory https://api.ebay.com/oauth/api_scope/sell.account https://api.ebay.com/oauth/api_scope/sell.fulfillment';

  try {
    const fetch = require('node-fetch');
    const r = await fetch('https://api.ebay.com/identity/v1/oauth2/token', {
      method: 'POST',
      headers: { 'Authorization': `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `grant_type=refresh_token&refresh_token=${encodeURIComponent(refreshToken)}&scope=${encodeURIComponent(SCOPES)}`,
    });
    const d = await r.json();
    if (d.access_token) {
      await db.setSetting('access_token', d.access_token);
      await db.setSetting('token_expiry', Date.now() + (d.expires_in * 1000));
      console.log('[Worker] Token refreshed');
      return d.access_token;
    }
    console.error('[Worker] Token refresh failed:', d.error_description);
    return null;
  } catch(e) {
    console.error('[Worker] Token refresh error:', e.message);
    return null;
  }
}

// ── Amazon scrape via Vercel ─────────────────────────────────────────────────
async function scrapeAmazonPrice(sourceUrl) {
  const fetch = require('node-fetch');
  const vercelUrl = process.env.VERCEL_BACKEND_URL || 'https://dropsync-one.vercel.app';
  try {
    const r = await fetch(`${vercelUrl}/api/ebay?action=scrape`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: sourceUrl }),
      timeout: 25000,
    });
    if (!r.ok) return null;
    const d = await r.json();
    if (d.price && d.price > 0) return d.price;
    return null;
  } catch(e) {
    console.error('[Worker] scrape error:', e.message);
    return null;
  }
}

// ── eBay price/stock update ───────────────────────────────────────────────────
async function updateEbayListing(token, ebaySku, newPrice, qty = 1) {
  const fetch = require('node-fetch');

  // Update price via Inventory API
  const priceBody = {
    availability: { shipToLocationAvailability: { quantity: qty } },
    condition: 'NEW',
  };

  // Use offer API to update price
  try {
    // Get offer for this SKU
    const offersR = await fetch(
      `https://api.ebay.com/sell/inventory/v1/offer?sku=${encodeURIComponent(ebaySku)}`,
      { headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json', 'Accept': 'application/json' } }
    );
    const offersD = await offersR.json();
    const offer = offersD.offers?.[0];
    if (!offer) return { success: false, error: 'No offer found' };

    // Update offer price
    const updateR = await fetch(
      `https://api.ebay.com/sell/inventory/v1/offer/${offer.offerId}`,
      {
        method: 'PUT',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...offer,
          pricingSummary: { price: { value: newPrice.toFixed(2), currency: 'USD' } },
        }),
      }
    );
    if (updateR.status === 204) return { success: true };
    const err = await updateR.json();
    return { success: false, error: JSON.stringify(err) };
  } catch(e) {
    return { success: false, error: e.message };
  }
}

async function setEbayQty(token, ebaySku, qty) {
  const fetch = require('node-fetch');
  try {
    const r = await fetch(
      `https://api.ebay.com/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`,
      {
        method: 'GET',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
      }
    );
    const item = await r.json();
    item.availability = { shipToLocationAvailability: { quantity: qty } };
    await fetch(
      `https://api.ebay.com/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`,
      {
        method: 'PUT',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(item),
      }
    );
  } catch(e) {
    console.error('[Worker] setEbayQty error:', e.message);
  }
}

// ── Sync one product ──────────────────────────────────────────────────────────
async function syncProduct(product, token) {
  const markup = parseFloat(await db.getSetting('markup') || 23);
  const result = { productId: product.id, title: product.title, status: 'ok', priceChanges: [], stockChanges: [], wentOos: false };

  try {
    const amazonPrice = await scrapeAmazonPrice(product.sourceUrl);
    if (amazonPrice === null) {
      result.status = 'scrape_failed';
      return result;
    }

    // Calculate new eBay price: (cost + $2) * (1 + markup%) / (1 - 0.1335) + $0.30
    const newPrice = parseFloat(((amazonPrice + 2) * (1 + markup / 100) / (1 - 0.1335) + 0.30).toFixed(2));
    const oldPrice = product.myPrice;

    const priceChanged = oldPrice && Math.abs(newPrice - oldPrice) >= 0.01;
    const oosNow = amazonPrice <= 0;

    if (oosNow) {
      // Set qty to 0 on eBay
      await setEbayQty(token, product.ebaySku, 0);
      result.wentOos = true;
      result.stockChanges.push(`${product.ebaySku}: qty→0 (OOS)`);
      await db.addLog('stock', `⚠️ Out of stock: ${product.title?.slice(0,50)}`, 'Quantity set to 0 on eBay', { productId: product.id });
    } else if (priceChanged) {
      const updated = await updateEbayListing(token, product.ebaySku, newPrice);
      if (updated.success) {
        result.priceChanges.push(`$${oldPrice?.toFixed(2)}→$${newPrice.toFixed(2)}`);
        await db.addLog('price', `Price updated: ${product.title?.slice(0,50)}`, `$${oldPrice?.toFixed(2)} → $${newPrice.toFixed(2)}`, { productId: product.id, oldPrice, newPrice });
      }
    } else {
      // Heartbeat — no changes
      await db.addLog('sync', `Monitored: ${product.title?.slice(0,50)}`, `No changes · $${newPrice.toFixed(2)}`, { productId: product.id });
    }

    // Update DB
    await db.updateProductSync(product.id, {
      myPrice: newPrice,
      amazonPrice,
      lastSynced: new Date().toISOString(),
      status: product.status,
      quantity: oosNow ? 0 : (product.quantity || 1),
    });

  } catch(e) {
    result.status = 'error';
    result.error = e.message;
    console.error(`[Worker] syncProduct error for ${product.id}:`, e.message);
  }

  return result;
}

// ── Main sync job ─────────────────────────────────────────────────────────────
async function runSync() {
  if (isSyncing) {
    console.log('[Worker] Sync already running, skipping');
    return;
  }
  isSyncing = true;

  try {
    const token = await getValidToken();
    if (!token) {
      console.log('[Worker] No valid token — skipping sync. Re-connect eBay in Settings.');
      await db.addLog('error', 'Sync skipped', 'No valid eBay token — re-connect eBay in Settings', {});
      return;
    }

    const products = await db.getProductsForSync(100);
    if (!products.length) {
      console.log('[Worker] No products to sync');
      return;
    }

    console.log(`[Worker] Syncing ${products.length} products...`);
    const results = { priceChanges: 0, stockChanges: 0, oos: 0, errors: 0 };

    for (const p of products) {
      const r = await syncProduct(p, token);
      if (r.priceChanges?.length) results.priceChanges += r.priceChanges.length;
      if (r.stockChanges?.length) results.stockChanges += r.stockChanges.length;
      if (r.wentOos) results.oos++;
      if (r.status !== 'ok') results.errors++;
      // Small delay between products to avoid rate limits
      await new Promise(res => setTimeout(res, 300));
    }

    console.log(`[Worker] Done — price:${results.priceChanges} stock:${results.stockChanges} oos:${results.oos} errors:${results.errors}`);
    await db.setSetting('last_sync_run', new Date().toISOString());
    await db.setSetting('last_sync_summary', results);

  } catch(e) {
    console.error('[Worker] Fatal sync error:', e.message);
  } finally {
    isSyncing = false;
  }
}

// ── Start scheduler ───────────────────────────────────────────────────────────
function startWorker() {
  console.log('[Worker] Starting — syncs every 5 minutes');

  // Run once on startup after 30s delay
  setTimeout(runSync, 30000);

  // Then every 5 minutes
  cron.schedule('*/5 * * * *', runSync);
}

module.exports = { startWorker, runSync, getValidToken };
