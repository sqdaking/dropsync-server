const cron  = require('node-cron');
const fetch = require('node-fetch');
const db    = require('./db');

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
      console.log('[Worker] Token refreshed successfully');
      return d.access_token;
    }
    console.error('[Worker] Token refresh failed:', d.error_description || d.error);
    return null;
  } catch(e) {
    console.error('[Worker] Token refresh error:', e.message);
    return null;
  }
}

// ── Webhook ───────────────────────────────────────────────────────────────────
// type: 'revise' | 'price' | 'stock' | 'oos' | 'error'
async function sendWebhook(webhookUrl, type, product, details = []) {
  if (!webhookUrl) return;
  const ebayUrl = product.ebayListingId ? `https://www.ebay.com/itm/${product.ebayListingId}` : '';
  const title   = (product.title || '').slice(0, 60);

  const icons  = { revise: '✅', price: '💰', stock: '📦', oos: '🚫', error: '❌' };
  const labels = { revise: 'Revised', price: 'Price Change', stock: 'Stock Change', oos: 'Out of Stock', error: 'Error' };
  const colors = { revise: 0x10b981, price: 0xf59e0b, stock: 0x8b5cf6, oos: 0xef4444, error: 0xef4444 };

  const icon  = icons[type]  || '🔄';
  const label = labels[type] || type;
  const color = colors[type] || 0x6b7280;

  const body = details.length ? details.join('\n') : label;
  const text = `${icon} *DropSync ${label}*\n*${title}*\n${body}${ebayUrl ? '\n' + ebayUrl : ''}`;

  const payload = {
    // Slack format
    text,
    // Discord format
    content: text.replace(/\*/g, '**'),
    embeds: [{
      title:       `${icon} ${label}: ${title}`,
      description: body,
      color,
      url:         ebayUrl || undefined,
      timestamp:   new Date().toISOString(),
      footer:      { text: 'DropSync' },
    }],
    // Generic
    type, product_title: product.title, details, ebay_url: ebayUrl,
    timestamp: new Date().toISOString(),
  };

  try {
    const r = await fetch(webhookUrl, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload), timeout: 8000,
    });
    console.log(`[Worker] Webhook sent (${type}): ${r.status}`);
  } catch(e) {
    console.warn('[Worker] Webhook failed:', e.message);
  }
}

// ── Revise one product ────────────────────────────────────────────────────────
async function reviseProduct(product, token, markup, handlingCost, webhookUrl) {
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
    const sku = product.ebaySku;
    if (!sku || !product.sourceUrl) {
      result.status = 'skipped_no_sku';
      return result;
    }

    console.log(`[Worker] Revising: "${(product.title||'').slice(0,50)}"`);

    const r = await fetch(`${VERCEL_URL}/api/ebay`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        action:         'revise',
        sandbox:        false,
        access_token:   token,
        ebaySku:        sku,
        sourceUrl:      product.sourceUrl,
        ebayListingId:  product.ebayListingId || '',
        markup,
        handlingCost,
        quantity:       product.quantity || 1,
        fallbackImages:  product.images     || [],
        fallbackTitle:   product.title      || '',
        fallbackPrice:   product.amazonPrice || product.cost || product.myPrice || 0,
        fallbackInStock: (product.quantity || 1) > 0,
      }),
      timeout: 180000, // 3 min max
    });

    // Handle non-JSON or network errors
    let data = null;
    try { data = r.ok ? await r.json() : await r.json().catch(() => null); } catch {}

    // 503 = Amazon blocked, no cached images — skip silently
    if (r.status === 503 || data?.skippable) {
      console.log(`[Worker]   skipped (blocked/no-cache): "${(product.title||'').slice(0,40)}"`);
      result.status = 'skipped_blocked';
      return result;
    }

    if (!r.ok || !data?.success) {
      const errText = data?.error || `HTTP ${r.status}`;
      console.warn(`[Worker]   FAILED: ${errText}`);
      result.status = 'revise_failed';
      result.error  = errText;
      await db.addLog('error',
        `Revise failed: ${(product.title||'').slice(0,50)}`,
        errText, { productId: product.id }
      );
      if (webhookUrl) await sendWebhook(webhookUrl, 'error', product, [errText]);
      return result;
    }

    // Success
    result.priceChanges = data.priceChanges || [];
    result.stockChanges = data.stockChanges || [];
    result.imageChanges = data.imageChanges || [];
    result.wentOos      = data.inStock === false;

    console.log(`[Worker]   ✓ ${data.type} · ${data.updatedVariants||1} variants · $${data.price?.toFixed(2)} · inStock=${data.inStock}`);

    // Update DB
    await db.updateProductSync(product.id, {
      myPrice:       data.price || product.myPrice,
      amazonPrice:   product.amazonPrice || 0,
      lastSynced:    new Date().toISOString(),
      status:        'listed',
      quantity:      data.inStock ? (product.quantity || 1) : 0,
      ebayListingId: product.ebayListingId || null,
    });

    // Log
    const logDetail = `${data.type==='variant'?`${data.updatedVariants} variants`:'1 item'} · ${data.images} imgs · $${data.price?.toFixed(2)} · ${data.inStock?'In Stock':'OOS'}`;
    await db.addLog('sync',
      `Revised: ${(product.title||'').slice(0,50)}`,
      logDetail, { productId: product.id }
    );

    // Webhook on every successful revise
    if (webhookUrl) {
      const details = [logDetail];
      if (result.priceChanges.length) details.push(...result.priceChanges.slice(0,5));
      if (result.stockChanges.length) details.push(...result.stockChanges.slice(0,5));
      const wType = result.wentOos ? 'oos'
        : result.priceChanges.length ? 'price'
        : result.stockChanges.length ? 'stock'
        : 'revise';
      await sendWebhook(webhookUrl, wType, product, details);
    }

  } catch(e) {
    result.status = 'error';
    result.error  = e.message;
    console.error(`[Worker] Exception for ${product.id}:`, e.message);
    await db.addLog('error',
      `Sync error: ${(product.title||'').slice(0,50)}`,
      e.message, { productId: product.id }
    );
  }

  return result;
}

// ── Main sync job ─────────────────────────────────────────────────────────────
async function runSync() {
  if (isSyncing) { console.log('[Worker] Already running, skipping'); return; }
  isSyncing = true;
  const cycleStart = Date.now();
  console.log('[Worker] ════ Sync cycle starting ════');

  try {
    const token = await getValidToken();
    if (!token) {
      console.log('[Worker] No valid eBay token — skipping');
      await db.addLog('error', 'Sync skipped', 'No valid eBay token — reconnect in Settings', {});
      return;
    }

    const markupRaw     = await db.getSetting('markup');
    const markup        = markupRaw != null ? parseFloat(markupRaw) : 0;
    const handlingCost  = parseFloat(await db.getSetting('handlingCost') || 2);
    const webhookUrl    = await db.getSetting('webhookUrl') || null;
    const products      = await db.getProductsForSync(30);

    if (!products.length) { console.log('[Worker] No products to sync'); return; }
    console.log(`[Worker] ${products.length} products · markup=${markup}% · handling=$${handlingCost}`);

    const totals = { ok: 0, errors: 0, skipped: 0, oos: 0 };

    for (const p of products) {
      const r = await reviseProduct(p, token, markup, handlingCost, webhookUrl);
      if (r.status === 'ok')                    { totals.ok++;      if (r.wentOos) totals.oos++; }
      else if (r.status.startsWith('skipped'))  totals.skipped++;
      else                                       totals.errors++;
      await sleep(3000);
    }

    const elapsed = ((Date.now() - cycleStart) / 1000).toFixed(0);
    const summary = `ok:${totals.ok} oos:${totals.oos} errors:${totals.errors} skipped:${totals.skipped} (${elapsed}s)`;
    console.log(`[Worker] ════ Done — ${summary} ════`);
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
  console.log('[Worker] Starting — cycles every 20 minutes');
  setTimeout(runSync, 30000);              // first run 30s after startup
  cron.schedule('*/20 * * * *', runSync); // then every 20 min
}

module.exports = { startWorker, runSync, getValidToken };
