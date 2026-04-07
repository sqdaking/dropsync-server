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

// ── Sync one product via smartSync (Amazon → eBay) ───────────────────────────
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

    // Rebuild skuToAsin from comboAsin if missing (old listings pushed before skuToAsin was tracked)
    let skuToAsin = product.skuToAsin && Object.keys(product.skuToAsin).length > 0 ? product.skuToAsin : null;
    if (!skuToAsin && product.comboAsin && Object.keys(product.comboAsin).length > 0) {
      const _normSku = sku.replace(/-[A-Z0-9_]+$/, '') === sku ? sku : sku; // group SKU
      const _pfx = _normSku + '-';
      const _SKU_MAX = 50;
      const _slug = s => (s||'').replace(/[^A-Z0-9]/gi,'_').toUpperCase().replace(/_+/g,'_').replace(/^_|_$/g,'');
      const _mkSku = parts => {
        const raw = parts.map(_slug).filter(Boolean).join('_');
        const maxSfx = _SKU_MAX - _pfx.length;
        if (raw.length <= maxSfx) return _pfx + raw;
        const hash = raw.split('').reduce((h,c) => ((h<<5)-h+c.charCodeAt(0))|0, 0);
        return _pfx + raw.slice(0, maxSfx-9) + '_' + (hash>>>0).toString(16).toUpperCase().padStart(8,'0');
      };
      skuToAsin = {};
      for (const [key, asin] of Object.entries(product.comboAsin)) {
        const [pv, sv] = key.split('|');
        const parts = [pv, sv].filter(Boolean);
        if (parts.length) skuToAsin[_mkSku(parts)] = asin;
      }
      console.log(`[Worker]   rebuilt skuToAsin: ${Object.keys(skuToAsin).length} entries`);
    }

    // Step 1: Scrape Amazon for fresh data
    let _scraped = null;
    try {
      const _sr = await fetch(`${VERCEL_URL}/api/ebay`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'scrape', url: product.sourceUrl }),
      });
      const _sd = await _sr.json().catch(() => null);
      if (_sd?.success && _sd?.product?.comboAsin) {
        _scraped = _sd.product;
        const _nC = Object.keys(_scraped.comboAsin || {}).length;
        const _nP = Object.values(_scraped.comboPrices || {}).filter(Boolean).length;
        console.log(`[Worker]   scraped: ${_nC} combos, ${_nP} prices`);
        // Rebuild skuToAsin from fresh comboAsin
        const _pfx2 = sku + '-';
        const _MAX2  = 50;
        const _sl2   = s => (s||'').replace(/[^A-Z0-9]/gi,'_').toUpperCase().replace(/_+/g,'_').replace(/^_|_$/g,'');
        const _mk2   = parts => {
          const raw = parts.map(_sl2).filter(Boolean).join('_');
          const max = _MAX2 - _pfx2.length;
          if (raw.length <= max) return _pfx2 + raw;
          const hash = raw.split('').reduce((h,c) => ((h<<5)-h+c.charCodeAt(0))|0, 0);
          return _pfx2 + raw.slice(0, max-9) + '_' + (hash>>>0).toString(16).toUpperCase().padStart(8,'0');
        };
        const _fS2A = {};
        for (const [key, asin] of Object.entries(_scraped.comboAsin)) {
          const [pv, sv] = key.split('|');
          const parts = [pv, sv].filter(Boolean);
          if (parts.length) _fS2A[_mk2(parts)] = asin;
        }
        if (Object.keys(_fS2A).length > 0) {
          skuToAsin = _fS2A;
          console.log(`[Worker]   refreshed skuToAsin: ${Object.keys(skuToAsin).length} entries`);
        }
        // Save fresh data back to DB
        await db.upsertProduct({
          ...product,
          comboAsin:         _scraped.comboAsin         || product.comboAsin,
          comboPrices:       _scraped.comboPrices       || product.comboPrices,
          comboInStock:      _scraped.comboInStock      || product.comboInStock,
          variations:        _scraped.variations        || product.variations,
          _primaryDimName:   _scraped._primaryDimName   || product._primaryDimName,
          _secondaryDimName: _scraped._secondaryDimName || product._secondaryDimName,
        });
      } else {
        console.log(`[Worker]   scrape blocked — using cached data`);
      }
    } catch(_se) {
      console.warn(`[Worker]   scrape error: ${_se.message}`);
    }

    // Step 2: smartSync with fresh scraped data
    const resp = await fetch(`${VERCEL_URL}/api/ebay`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        action:        'smartSync',
        access_token:  token,
        ebaySku:       sku,
        ebayListingId: product.ebayListingId || '',
        sourceUrl:     product.sourceUrl,
        markup,
        handlingCost,
        quantity:      product.quantity || 1,
        skuToAsin:          skuToAsin && Object.keys(skuToAsin).length <= 500 ? skuToAsin : null,
        cachedOfferIds:     product.offerIdCache && Object.keys(product.offerIdCache).length <= 500 ? product.offerIdCache : null,
        comboAsin:                  (_scraped?.comboAsin         || product.comboAsin)         || null,
        fallbackComboPrices:        (_scraped?.comboPrices       || product.comboPrices)       || null,
        fallbackComboInStock:       (_scraped?.comboInStock      || product.comboInStock)      || null,
        fallbackVariations:         (_scraped?.variations        || product.variations)        || null,
        fallbackPrimaryDimName:     (_scraped?._primaryDimName   || product._primaryDimName)   || null,
        fallbackSecondaryDimName:   (_scraped?._secondaryDimName || product._secondaryDimName) || null,
        fallbackPrice:              _scraped?.price || product.cost || 0,
        fallbackShipping:           product.shippingCost || 0,
      }),
    });
    const data = await resp.json().catch(() => ({}));

    if (!data.success) {
      // Stale offer ID cache — clear it so next cycle does fresh discovery
      if (data.offersCacheStale) {
        await db.upsertProduct({ ...product, offerIdCache: null, offerIdCacheAt: null });
        console.log(`[Worker]   stale offer cache cleared — will rediscover next cycle`);
        result.status = 'cache_cleared';
        return result;
      }
      if (data.skippable) { result.status = 'skipped_blocked'; return result; }
      const errText = data.error || 'smartSync failed';
      console.warn(`[Worker]   FAILED: ${errText}`);
      result.status = 'revise_failed'; result.error = errText;
      await db.addLog('error', `Sync failed: ${(product.title||'').slice(0,50)}`, errText, { productId: product.id });
      if (webhookUrl) await sendWebhook(webhookUrl, 'error', product, [errText]);
      return result;
    }

    const synced  = data.synced  || 0;
    const prices  = data.prices  || {};
    const inStock = Object.values(data.inStock || {}).some(Boolean);
    result.wentOos = !inStock;

    const priceList = Object.values(prices).filter(Boolean);
    const avgPrice  = priceList.length ? priceList.reduce((a, b) => a + b, 0) / priceList.length : 0;
    console.log(`[Worker]   ✓ ${synced} offers updated · avg Amazon $${avgPrice.toFixed(2)} · ${inStock ? 'In Stock' : 'OOS'}`);

    await db.updateProductSync(product.id, {
      lastSynced:    new Date().toISOString(),
      status:        'listed',
      ebayListingId: product.ebayListingId || null,
    });

    if (result.wentOos) {
      await db.addLog('sync', `OOS: ${(product.title||'').slice(0,50)}`, `${synced} offers updated`, { productId: product.id });
    }

    if (webhookUrl) {
      const wType = result.wentOos ? 'oos' : 'revise';
      await sendWebhook(webhookUrl, wType, product, [`${synced} offers synced · avg $${avgPrice.toFixed(2)}`]);
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

      // ── Pause check — user can pause from Settings ──────────────────────────
      const _paused = (await db.getSetting('worker_paused')) === 'true';
      if (_paused) {
        console.log('[Worker] Paused — waiting 15s before checking again');
        await sleep(15000);
        continue;
      }

      // ── Push lock — frontend is pushing a listing, hold off ──────────────────
      const _pushLock = (await db.getSetting('push_lock')) === 'true';
      if (_pushLock) {
        console.log('[Worker] Push in progress — waiting 10s');
        await sleep(10000);
        continue;
      }

      // ── Priority: process frontend edit+sync requests immediately ─────────────
      try {
        const _pq = await db.pool.query(
          "SELECT id, data FROM products WHERE data->>'sync_pending' = 'true' LIMIT 5"
        );
        if (_pq.rows.length > 0) {
          const _pMarkup   = parseFloat(await db.getSetting('markup') || '0') || 0;
          const _pHandling = parseFloat(await db.getSetting('handlingCost') || '2') || 2;
          const _pWebhook  = await db.getSetting('webhookUrl') || null;
          for (const row of _pq.rows) {
            const prod = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;
            if (!prod.id) prod.id = row.id;
            console.log(`[Worker] ⚡ Priority sync (edit): "${(prod.title||'').slice(0,45)}"`);
            await db.upsertProduct({ ...prod, sync_pending: false }); // clear flag first
            _cachedListed = null; // force fresh rotation cache
            const effMarkup = (prod.markup != null && prod.markup >= 0) ? prod.markup : _pMarkup;
            await reviseProduct(prod, token, effMarkup, _pHandling, _pWebhook);
          }
          continue; // restart loop — don't advance normal cursor
        }
      } catch (_pqErr) {
        console.warn('[Worker] Priority sync check error:', _pqErr.message);
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

      _reviseStart = Date.now();
      const markupRaw    = await db.getSetting('markup');
      const globalMarkup = markupRaw != null ? parseFloat(markupRaw) : 0;
      const handlingCost = parseFloat(await db.getSetting('handlingCost') || 2);
      const webhookUrl   = await db.getSetting('webhookUrl') || null;

      // ── Concurrency: revise N products in parallel ──────────────────────────
      const concurrency = Math.min(Math.max(parseInt(await db.getSetting('worker_concurrency') || '1') || 1, 1), 10);
      const batch = listed.slice(cursor, cursor + concurrency);

      if (concurrency > 1) {
        console.log(`[Worker] [${cursor + 1}-${cursor + batch.length}/${listed.length}] Revising ${batch.length} in parallel (concurrency=${concurrency})`);
      } else {
        console.log(`[Worker] [${cursor + 1}/${listed.length}] Revising: "${(batch[0]?.title||'').slice(0,45)}"`);
      }

      await Promise.all(batch.map(product => {
        const effectiveMarkup = (product.markup != null && product.markup > 0) ? product.markup : globalMarkup;
        return reviseProduct(product, token, effectiveMarkup, handlingCost, webhookUrl);
      }));

      cursor += batch.length;
      await db.setSetting('revise_cursor', String(cursor));

    } catch(e) {
      console.error('[Worker] Loop error:', e.message);
      await db.addLog('error', 'Worker loop error', e.message, {}).catch(() => {});
      await sleep(10000); // brief pause on unexpected errors
    }

    // Adaptive spacing between revises to avoid Amazon rate limiting.
    // Target: ~60s between revise starts — gives Amazon IP cooldown time.
    // Also check for push lock: if frontend is pushing, wait it out.
    const _reviseDuration = Date.now() - _reviseStart;
    const _minGap = 60000; // 60s minimum between revise starts
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
