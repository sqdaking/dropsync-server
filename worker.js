const cron  = require('node-cron');
const fetch  = require('node-fetch');
const db     = require('./db');

let isSyncing = false;

const VERCEL_URL = process.env.VERCEL_BACKEND_URL || 'https://dropsync-one.vercel.app';
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ── Token management ──────────────────────────────────────────────────────────
async function getValidToken() {
  const accessToken  = await db.getSetting('access_token');
  const tokenExpiry  = await db.getSetting('token_expiry');
  const refreshToken = await db.getSetting('refresh_token');

  if (accessToken && tokenExpiry && Date.now() < parseInt(tokenExpiry) - 60000) return accessToken;
  if (!refreshToken) return null;

  const clientId     = process.env.EBAY_CLIENT_ID;
  const clientSecret = process.env.EBAY_CLIENT_SECRET;
  if (!clientId || !clientSecret) return null;

  const creds  = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
  const SCOPES = [
    'https://api.ebay.com/oauth/api_scope',
    'https://api.ebay.com/oauth/api_scope/sell.inventory',
    'https://api.ebay.com/oauth/api_scope/sell.account',
    'https://api.ebay.com/oauth/api_scope/sell.fulfillment',
  ].join(' ');

  try {
    const r = await fetch('https://api.ebay.com/identity/v1/oauth2/token', {
      method: 'POST',
      headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `grant_type=refresh_token&refresh_token=${encodeURIComponent(refreshToken)}&scope=${encodeURIComponent(SCOPES)}`,
    });
    const d = await r.json();
    if (d.access_token) {
      await db.setSetting('access_token', d.access_token);
      await db.setSetting('token_expiry', String(Date.now() + d.expires_in * 1000));
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

// ── Webhook ───────────────────────────────────────────────────────────────────
async function sendWebhook(webhookUrl, type, product, changes) {
  if (!webhookUrl || !changes.length) return;
  const emoji   = type === 'price' ? '💰' : type === 'stock' ? '📦' : '🖼️';
  const label   = type === 'price' ? 'Price Change' : type === 'stock' ? 'Stock Change' : 'Images Updated';
  const ebayUrl = product.ebayListingId ? `https://www.ebay.com/itm/${product.ebayListingId}` : '';
  const payload = {
    text: `${emoji} *DropSync ${label}*\n*${(product.title||'').slice(0,60)}*\n${changes.join('\n')}${ebayUrl?'\n'+ebayUrl:''}`,
    content: `${emoji} **DropSync ${label}**\n**${(product.title||'').slice(0,60)}**\n${changes.join('\n')}${ebayUrl?'\n<'+ebayUrl+'>':''}`,
    embeds: [{
      title: `${emoji} ${label}: ${(product.title||'').slice(0,60)}`,
      description: changes.join('\n'),
      color: type === 'price' ? 0xf59e0b : type === 'stock' ? 0x8b5cf6 : 0x3b82f6,
      url: ebayUrl || undefined,
      timestamp: new Date().toISOString(),
    }],
    type, product_title: product.title, changes, ebay_url: ebayUrl,
    timestamp: new Date().toISOString(),
  };
  try {
    await fetch(webhookUrl, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload), timeout: 8000,
    });
  } catch(e) { console.warn('[Worker] Webhook failed:', e.message); }
}

// ── Revise one product via Vercel revise action ──────────────────────────────
// Full in-place replacement: scrape Amazon → wipe+replace title, images, qty, price
// Keeps the same eBay listing ID — listing stays live throughout
async function syncProduct(product, token, markup, webhookUrl) {
  const result = {
    productId:    product.id,
    title:        product.title,
    status:       'ok',
    priceChanges: [],
    stockChanges: [],
    imageChanges: [],
    wentOos:      false,
  };

  try {
    const groupSku = product.ebaySku;
    if (!groupSku || !product.sourceUrl) {
      result.status = 'skipped_no_sku';
      return result;
    }

    console.log(`[Worker] Revising: "${(product.title||'').slice(0,50)}"`);
    console.log(`[Worker]   sku=${groupSku.slice(0,35)} url=${product.sourceUrl.slice(0,50)}`);

    // Call Vercel sync action — it does everything (scrape + full inventory PUT + price update)
    const r = await fetch(`${VERCEL_URL}/api/ebay`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        action:        'revise',
        sandbox:       false,
        access_token:  token,
        ebaySku:       groupSku,
        sourceUrl:     product.sourceUrl,
        ebayListingId: product.ebayListingId || product.ebay_item_id || '',
        markup:        markup,
        handlingCost:  parseFloat(await db.getSetting('handlingCost') || 2),
        quantity:      product.quantity || 1,
        // Cached fallback data in case Amazon blocks the scrape
        fallbackImages:  product.images || [],
        fallbackTitle:   product.title  || '',
        fallbackPrice:   product.amazonPrice || product.cost || 0,
        fallbackInStock: product.quantity > 0,
      }),
      timeout: 120000, // sync can take up to 2 min for large variation listings
    });

    const data = r.ok ? await r.json().catch(() => null) : null;

    if (!r.ok || !data?.success) {
      // 503 = Amazon blocked with no cache — skip silently, retry next cycle
      if (r.status === 503 || data?.skippable) {
        console.log(`[Worker]   skipped (Amazon blocked, no cache): ${(product.title||'').slice(0,40)}`);
        result.status = 'skipped_blocked';
        return result;
      }
      const errText = data?.error || `HTTP ${r.status}`;
      console.warn(`[Worker]   revise failed: ${errText}`);
      result.status = 'revise_failed';
      result.error  = errText;
      await db.addLog('error',
        `Revise failed: ${(product.title||'').slice(0,50)}`,
        errText,
        { productId: product.id }
      );
      return result;
    }

    // Extract change diffs from response
    result.priceChanges = data.priceChanges || [];
    result.stockChanges = data.stockChanges || [];
    result.imageChanges = data.imageChanges || [];
    result.wentOos      = data.inStock === false;

    const hasChanges = result.priceChanges.length || result.stockChanges.length || result.imageChanges.length;

    console.log(`[Worker]   ✓ type=${data.type} imgs=${data.images} price=$${data.price?.toFixed(2)} inStock=${data.inStock}`);
    if (result.priceChanges.length) console.log(`[Worker]   price changes: ${result.priceChanges.slice(0,5).join(', ')}`);
    if (result.stockChanges.length) console.log(`[Worker]   stock changes: ${result.stockChanges.slice(0,5).join(', ')}`);

    // Update DB with fresh values
    await db.updateProductSync(product.id, {
      myPrice:       data.price || product.myPrice,
      amazonPrice:   product.amazonPrice,
      lastSynced:    new Date().toISOString(),
      status:        product.status,
      quantity:      data.inStock ? (product.quantity || 1) : 0,
      ebayListingId: product.ebayListingId || product.ebay_item_id || null,
    });

    // Log
    if (!hasChanges) {
      await db.addLog('sync',
        `Monitored: ${(product.title||'').slice(0,50)}`,
        `No changes · ${data.type==='variation'?`${data.updatedVariants} variants`:'simple'} · $${data.price?.toFixed(2)} · ${data.images} imgs`,
        { productId: product.id }
      );
    } else {
      if (result.priceChanges.length) {
        await db.addLog('price',
          `Prices updated: ${(product.title||'').slice(0,50)}`,
          result.priceChanges.slice(0, 8).join(' · '),
          { productId: product.id }
        );
      }
      if (result.stockChanges.length) {
        await db.addLog('stock',
          `Stock updated: ${(product.title||'').slice(0,50)}`,
          [...new Set(result.stockChanges)].join(' · '),
          { productId: product.id }
        );
      }
      if (result.imageChanges.length) {
        await db.addLog('sync',
          `Images updated: ${(product.title||'').slice(0,50)}`,
          result.imageChanges.join(' · '),
          { productId: product.id }
        );
      }
    }

    // Webhooks for actual changes
    if (webhookUrl) {
      if (result.priceChanges.length) await sendWebhook(webhookUrl, 'price', product, result.priceChanges.slice(0, 10));
      if (result.stockChanges.length) await sendWebhook(webhookUrl, 'stock', product, [...new Set(result.stockChanges)]);
      if (result.imageChanges.length) await sendWebhook(webhookUrl, 'image', product, result.imageChanges);
    }

  } catch(e) {
    result.status = 'error';
    result.error  = e.message;
    console.error(`[Worker] syncProduct error ${product.id}:`, e.message);
    await db.addLog('error',
      `Sync error: ${(product.title||'').slice(0,50)}`,
      e.message,
      { productId: product.id }
    );
  }

  return result;
}

// ── Main sync job ─────────────────────────────────────────────────────────────
async function runSync() {
  if (isSyncing) { console.log('[Worker] Already running, skipping'); return; }
  isSyncing = true;
  console.log('[Worker] ═══════════════ Sync cycle starting ═══════════════');

  try {
    const token = await getValidToken();
    if (!token) {
      console.log('[Worker] No valid token — skipping sync');
      await db.addLog('error', 'Sync skipped', 'No valid eBay token — re-connect in Settings', {});
      return;
    }

    const markupRaw  = await db.getSetting('markup');
    const markup     = (markupRaw !== null && markupRaw !== undefined) ? parseFloat(markupRaw) : 0;
    if (!markup) {
      console.warn('[Worker] markup not set in DB — run Settings → Save Pricing to sync it from the app');
    }
    console.log(`[Worker] markup=${markup}%`);
    const webhookUrl = await db.getSetting('webhookUrl') || null;
    const products   = await db.getProductsForSync(30); // 30 at a time (variation syncs are heavy)

    if (!products.length) { console.log('[Worker] No products to sync'); return; }
    console.log(`[Worker] ${products.length} products queued · markup=${markup}%`);

    const totals = { priceChanges: 0, stockChanges: 0, imageChanges: 0, oos: 0, errors: 0, skipped: 0 };

    for (const p of products) {
      // Skip placeholder/unrecovered SKUs
      if (!p.ebaySku || p.ebaySku.startsWith('DS-RECOVERED') || p.ebaySku.startsWith('DS-EXISTING')) {
        totals.skipped++;
        console.log(`[Worker] Skip placeholder SKU: ${p.ebaySku}`);
        continue;
      }

      const r = await syncProduct(p, token, markup, webhookUrl);

      totals.priceChanges += r.priceChanges?.length || 0;
      totals.stockChanges += r.stockChanges?.length || 0;
      totals.imageChanges += r.imageChanges?.length || 0;
      if (r.wentOos) totals.oos++;
      if (r.status !== 'ok') {
        if (r.status.startsWith('skipped')) totals.skipped++;
        else totals.errors++;
      }

      // Pace between products — sync is heavy (scrape + ~250 API calls for variation)
      await sleep(3000);
    }

    const summary = `price:${totals.priceChanges} stock:${totals.stockChanges} images:${totals.imageChanges} oos:${totals.oos} errors:${totals.errors} skipped:${totals.skipped}`;
    console.log(`[Worker] ═══ Done — ${summary} ═══`);
    await db.setSetting('last_sync_run', new Date().toISOString());
    await db.setSetting('last_sync_summary', totals);
    await db.addLog('sync', 'Auto-sync complete', summary, {});

  } catch(e) {
    console.error('[Worker] Fatal error:', e.message);
    await db.addLog('error', 'Auto-sync fatal', e.message, {}).catch(() => {});
  } finally {
    isSyncing = false;
  }
}

// ── Start scheduler ───────────────────────────────────────────────────────────
function startWorker() {
  console.log('[Worker] Starting — syncs every 20 minutes');
  setTimeout(runSync, 45000);             // first run 45s after startup
  cron.schedule('*/20 * * * *', runSync); // then every 20 min
}

module.exports = { startWorker, runSync, getValidToken };
