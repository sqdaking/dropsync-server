const fetch = require('node-fetch');
const db    = require('./db');

const VERCEL_URL = process.env.VERCEL_BACKEND_URL || 'https://dropsync-one.vercel.app';
const sleep = ms => new Promise(r => setTimeout(r, ms));

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
    console.error('[Worker] Token refresh failed:', d.error_description || d.error);
    return null;
  } catch(e) {
    console.error('[Worker] Token refresh error:', e.message);
    return null;
  }
}

// ── Webhook ───────────────────────────────────────────────────────────────────
async function sendWebhook(webhookUrl, type, product, details = []) {
  if (!webhookUrl) return;
  const ebayUrl = product.ebayListingId ? `https://www.ebay.com/itm/${product.ebayListingId}` : '';
  const title   = (product.title || '').slice(0, 60);
  const icons   = { revise: '✅', price: '💰', stock: '📦', oos: '🚫', error: '❌' };
  const labels  = { revise: 'Revised', price: 'Price Change', stock: 'Stock Change', oos: 'Out of Stock', error: 'Error' };
  const colors  = { revise: 0x10b981, price: 0xf59e0b, stock: 0x8b5cf6, oos: 0xef4444, error: 0xef4444 };
  const icon    = icons[type] || '🔄';
  const label   = labels[type] || type;
  const color   = colors[type] || 0x6b7280;
  const body    = details.length ? details.join('\n') : label;
  const text    = `${icon} *DropSync ${label}*\n*${title}*\n${body}${ebayUrl ? '\n' + ebayUrl : ''}`;
  const payload = {
    text, content: text.replace(/\*/g, '**'),
    embeds: [{ title: `${icon} ${label}: ${title}`, description: body, color, url: ebayUrl || undefined, timestamp: new Date().toISOString(), footer: { text: 'DropSync' } }],
    type, product_title: product.title, details, ebay_url: ebayUrl, timestamp: new Date().toISOString(),
  };
  try {
    await fetch(webhookUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload), timeout: 8000 });
  } catch(e) { console.warn('[Worker] Webhook failed:', e.message); }
}

// ── Revise one product ────────────────────────────────────────────────────────
async function reviseProduct(product, token, markup, handlingCost, webhookUrl) {
  const result = { productId: product.id, title: product.title, status: 'ok', priceChanges: [], stockChanges: [], wentOos: false };

  try {
    const sku = product.ebaySku;
    if (product.sourceUrl && /aliexpress\.com/i.test(product.sourceUrl)) {
      result.status = 'skipped_ae'; return result;
    }
    if (!sku || !product.sourceUrl) { result.status = 'skipped_no_sku'; return result; }
    if (product.markedOos === true) {
      console.log(`[Worker]   skipping OOS-locked: "${(product.title||'').slice(0,40)}"`);
      result.status = 'skipped_oos'; return result;
    }

    console.log(`[Worker] → "${(product.title||'').slice(0,50)}"`);

    const r = await fetch(`${VERCEL_URL}/api/ebay`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      // No X-Last-Revised — no cooldown, revise every time it comes up in rotation
      body: JSON.stringify({
        action:       'revise',
        access_token: token,
        ebaySku:      sku,
        sourceUrl:    product.sourceUrl,
        markup,
        handlingCost,
        quantity:     product.quantity || 1,
        // comboAsin locks which ASINs to fetch from Amazon (no random re-trimming)
        comboAsin: product.comboAsin && Object.keys(product.comboAsin).length <= 500
          ? product.comboAsin : null,
      }),
      timeout: 290000, // just under Vercel's 300s limit
    });

    let data = null;
    try { data = await r.json(); } catch {}

    if (r.status === 503 || data?.skippable) {
      console.log(`[Worker]   skipped (blocked/no-cache)`);
      result.status = 'skipped_blocked'; return result;
    }

    if (!r.ok || !data?.success) {
      const errText = data?.error || `HTTP ${r.status}`;
      console.warn(`[Worker]   FAILED: ${errText}`);
      result.status = 'revise_failed'; result.error = errText;
      await db.addLog('error', `Revise failed: ${(product.title||'').slice(0,50)}`, errText, { productId: product.id });
      if (webhookUrl) await sendWebhook(webhookUrl, 'error', product, [errText]);
      return result;
    }

    result.priceChanges = data.priceChanges || [];
    result.stockChanges = data.stockChanges || [];
    result.wentOos      = data.inStock === false;

    const inStockStr = data.inStock ? 'In Stock' : 'OOS';
    console.log(`[Worker]   ✓ ${data.updatedVariants||1} variants · $${(data.price||0).toFixed(2)} · ${inStockStr} · priceChanges=${result.priceChanges.length}`);

    await db.updateProductSync(product.id, {
      myPrice:       data.price || product.myPrice,
      lastSynced:    new Date().toISOString(),
      status:        'listed',
      quantity:      data.inStock ? (product.quantity || 1) : 0,
      ebayListingId: product.ebayListingId || null,
    });

    const logDetail = `${data.updatedVariants||1} variants · $${(data.price||0).toFixed(2)} · ${inStockStr}`;
    // Only log OOS transitions and errors — logging every successful revise fills the DB
    if (result.wentOos) {
      await db.addLog('sync', `OOS: ${(product.title||'').slice(0,50)}`, logDetail, { productId: product.id });
    }

    if (webhookUrl) {
      const wType = result.wentOos ? 'oos' : result.priceChanges.length ? 'price' : result.stockChanges.length ? 'stock' : 'revise';
      const details = [logDetail, ...result.priceChanges.slice(0,5), ...result.stockChanges.slice(0,5)];
      await sendWebhook(webhookUrl, wType, product, details);
    }

  } catch(e) {
    result.status = 'error'; result.error = e.message;
    console.error(`[Worker] Exception:`, e.message);
    await db.addLog('error', `Sync error: ${(product.title||'').slice(0,50)}`, e.message, { productId: product.id });
  }

  return result;
}

// ── Continuous rotation loop ──────────────────────────────────────────────────
// Revises one listing at a time, forever, from index 0 → N → 0 → ...
// No cooldown, no batch size, no cron. One at a time, always moving forward.
async function runForever() {
  console.log('[Worker] ════ Continuous rotation started ════');

  // Restore cursor from DB (survives restarts)
  let cursor = parseInt(await db.getSetting('revise_cursor') || '0') || 0;
  let _cachedListed = null;   // cached product list — only reloaded on wrap
  let _cacheLoadedAt = 0;

  while (true) {
    let _reviseStart = Date.now(); // declared here so spacing code always has it
    try {
      // Refresh token if needed
      const token = await getValidToken();
      if (!token) {
        console.log('[Worker] No valid eBay token — waiting 60s');
        await sleep(60000);
        continue;
      }

      // Reload product list only when cursor wraps or cache is stale (>10min)
      // Loading 3000 products every 30s was the main memory leak
      const _cacheStale = Date.now() - _cacheLoadedAt > 600000; // 10min
      if (!_cachedListed || cursor === 0 || _cacheStale) {
        const allProducts = await db.getProductsForSync(9999);
        _cachedListed = allProducts.filter(p =>
          p.status === 'listed' && p.ebaySku && p.sourceUrl &&
          !/aliexpress\.com/i.test(p.sourceUrl) &&
          p.markedOos !== true
        );
        _cacheLoadedAt = Date.now();
        if (cursor === 0) console.log(`[Worker] Product list loaded: ${_cachedListed.length} listed`);
      }
      const listed = _cachedListed;

      if (!listed.length) {
        // Back off: 30s → 60s → 120s → ... → 600s max
        // Avoids hammering Railway DB when catalog is empty
        if (typeof _emptyCount === 'undefined') _emptyCount = 0;
        _emptyCount++;
        const _backoff = Math.min(30000 * Math.pow(2, _emptyCount - 1), 600000);
        console.log(`[Worker] No listed products — waiting ${Math.round(_backoff/1000)}s (check #${_emptyCount})`);
        _cachedListed = null;
        await sleep(_backoff);
        continue;
      }
      // Reset empty counter when products found
      if (typeof _emptyCount !== 'undefined') _emptyCount = 0;

      // Wrap cursor
      if (cursor >= listed.length) {
        cursor = 0;
        _cachedListed = null; // force fresh reload on next wrap
        console.log(`[Worker] ── Rotation complete — restarting from 0 (${listed.length} listings) ──`);
        await db.setSetting('last_sync_run', new Date().toISOString());
        continue;
      }

      const product = listed[cursor];
      _reviseStart = Date.now(); // reset to actual revise start (after cache/cursor setup)
      const markupRaw    = await db.getSetting('markup');
      const globalMarkup = markupRaw != null ? parseFloat(markupRaw) : 0;
      const handlingCost = parseFloat(await db.getSetting('handlingCost') || 2);
      const webhookUrl   = await db.getSetting('webhookUrl') || null;
      const effectiveMarkup = (product.markup != null && product.markup > 0) ? product.markup : globalMarkup;

      console.log(`[Worker] [${cursor + 1}/${listed.length}] Revising: "${(product.title||'').slice(0,45)}"`);
      await reviseProduct(product, token, effectiveMarkup, handlingCost, webhookUrl);

      cursor++;
      await db.setSetting('revise_cursor', String(cursor));

    } catch(e) {
      console.error('[Worker] Loop error:', e.message);
      await db.addLog('error', 'Worker loop error', e.message, {}).catch(() => {});
      await sleep(10000); // brief pause on unexpected errors
    }

    // Adaptive spacing between revises to avoid Amazon rate limiting.
    // Each revise hits Amazon from a fresh Vercel IP, but back-to-back calls
    // on the same product range can still trigger blocks.
    // Target: ~30s between revises (most listings take 20-60s to complete,
    // so the natural revise time already provides most of the gap).
    const _reviseDuration = Date.now() - _reviseStart;
    const _minGap = 30000; // 30s minimum between revise starts
    const _remaining = _minGap - _reviseDuration;
    if (_remaining > 0) {
      console.log(`[Worker] Spacing: waiting ${(_remaining/1000).toFixed(0)}s (revise took ${(_reviseDuration/1000).toFixed(0)}s)`);
      await sleep(_remaining);
    }
  }
}

// ── Start ─────────────────────────────────────────────────────────────────────
function startWorker() {
  console.log('[Worker] Starting continuous rotation (no cooldown, one at a time)');
  // Brief startup delay so DB/server can initialize
  setTimeout(runForever, 5000);
}

module.exports = { startWorker, runForever, getValidToken };
