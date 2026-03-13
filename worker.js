const cron  = require('node-cron');
const fetch  = require('node-fetch');
const db     = require('./db');

let isSyncing = false;

const EBAY_API   = 'https://api.ebay.com';
const VERCEL_URL = process.env.VERCEL_BACKEND_URL || 'https://dropsync-one.vercel.app';

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ── Price formula (matches ebay.js exactly) ───────────────────────────────────
function applyMarkup(cost, markupPct, handling = 2) {
  const c = parseFloat(cost) || 0;
  if (c <= 0) return 0;
  const ebayFee = 0.1335;
  return Math.max(
    Math.ceil(((c + handling) * (1 + markupPct / 100) / (1 - ebayFee) + 0.30) * 100) / 100,
    0.99
  );
}

// ── Token management ──────────────────────────────────────────────────────────
async function getValidToken() {
  const accessToken = await db.getSetting('access_token');
  const tokenExpiry = await db.getSetting('token_expiry');
  const refreshToken = await db.getSetting('refresh_token');

  if (accessToken && tokenExpiry && Date.now() < tokenExpiry - 60000) return accessToken;
  if (!refreshToken) return null;

  const clientId     = process.env.EBAY_CLIENT_ID;
  const clientSecret = process.env.EBAY_CLIENT_SECRET;
  if (!clientId || !clientSecret) return null;

  const creds  = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
  const SCOPES = 'https://api.ebay.com/oauth/api_scope https://api.ebay.com/oauth/api_scope/sell.inventory https://api.ebay.com/oauth/api_scope/sell.account https://api.ebay.com/oauth/api_scope/sell.fulfillment';

  try {
    const r = await fetch('https://api.ebay.com/identity/v1/oauth2/token', {
      method: 'POST',
      headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `grant_type=refresh_token&refresh_token=${encodeURIComponent(refreshToken)}&scope=${encodeURIComponent(SCOPES)}`,
    });
    const d = await r.json();
    if (d.access_token) {
      await db.setSetting('access_token', d.access_token);
      await db.setSetting('token_expiry', Date.now() + d.expires_in * 1000);
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
  if (!webhookUrl) return;
  const emoji = type === 'price' ? '💰' : '📦';
  const label = type === 'price' ? 'Price Change' : 'Stock Change';
  const ebayUrl = product.ebayListingId ? `https://www.ebay.com/itm/${product.ebayListingId}` : '';
  const payload = {
    text: `${emoji} *DropSync ${label}*\n*${(product.title||'').slice(0,60)}*\n${changes.join('\n')}${ebayUrl?'\n'+ebayUrl:''}`,
    content: `${emoji} **DropSync ${label}**\n**${(product.title||'').slice(0,60)}**\n${changes.join('\n')}${ebayUrl?'\n<'+ebayUrl+'>':''}`,
    embeds: [{
      title: `${emoji} ${label}: ${(product.title||'').slice(0,60)}`,
      description: changes.join('\n'),
      color: type === 'price' ? 0xf59e0b : 0x8b5cf6,
      url: ebayUrl || undefined,
      timestamp: new Date().toISOString(),
    }],
    type, product_title: product.title, changes, ebay_url: ebayUrl,
    timestamp: new Date().toISOString(),
  };
  try {
    await fetch(webhookUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload), timeout: 8000 });
  } catch(e) { console.warn('[Worker] Webhook failed:', e.message); }
}

// ── Scrape Amazon via Vercel ──────────────────────────────────────────────────
async function scrapeAsin(asin) {
  try {
    const r = await fetch(`${VERCEL_URL}/api/ebay?action=scrape`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: `https://www.amazon.com/dp/${asin}?th=1` }),
      timeout: 30000,
    });
    if (!r.ok) return null;
    const d = await r.json();
    return d.product || null;
  } catch(e) { console.warn(`[Worker] scrape ${asin} failed:`, e.message); return null; }
}

// ── eBay API helpers ──────────────────────────────────────────────────────────
const ebayAuth = (token) => ({ Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' });
const ebayAuthLang = (token) => ({ ...ebayAuth(token), 'Content-Language': 'en-US' });

async function getInventoryGroup(token, groupSku) {
  const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`, { headers: ebayAuth(token) });
  return r.ok ? r.json() : null;
}

async function getInventoryItem(token, sku) {
  const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, { headers: ebayAuth(token) });
  return r.ok ? r.json() : null;
}

async function putInventoryItem(token, sku, body) {
  const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, { method: 'PUT', headers: ebayAuthLang(token), body: JSON.stringify(body) });
  if (!r.ok) {
    const err = await r.json().catch(()=>({}));
    console.warn(`[Worker] item PUT failed ${sku.slice(-25)}:`, err.errors?.[0]?.message?.slice(0,80));
    return false;
  }
  return true;
}

async function getOffer(token, sku) {
  const r = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}`, { headers: ebayAuth(token) });
  const d = r.ok ? await r.json() : {};
  return (d.offers||[])[0] || null;
}

async function putOfferPrice(token, offerId, price) {
  const r = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerId}`, {
    method: 'PUT', headers: ebayAuth(token),
    body: JSON.stringify({ pricingSummary: { price: { value: price.toFixed(2), currency: 'USD' } } }),
  });
  return r.status === 204 || r.ok;
}

// ── Parse the color/size suffix from a variant SKU ────────────────────────────
function parseSkuSuffix(sku, groupSku) {
  if (sku.startsWith(groupSku + '-')) return sku.slice(groupSku.length + 1).toLowerCase().replace(/_/g, ' ');
  return sku.toLowerCase();
}

// Match a SKU suffix to the best color name from the ASIN map
function matchColor(skuSuffix, colorAsinMap) {
  const colors = Object.keys(colorAsinMap);
  if (!colors.length) return null;
  const s = skuSuffix.replace(/\s/g, '');
  for (const color of colors) {
    const cn = color.toLowerCase().replace(/[^a-z0-9]/g,'');
    if (s.startsWith(cn.slice(0,8)) || cn.startsWith(s.slice(0,8))) return color;
  }
  return null;
}

// ── Sync one product ──────────────────────────────────────────────────────────
async function syncProduct(product, token, markup, webhookUrl) {
  const result = { productId: product.id, title: product.title, status: 'ok', priceChanges: [], stockChanges: [], wentOos: false };

  try {
    const groupSku = product.ebaySku;
    const asinMatch = (product.sourceUrl||'').match(/\/dp\/([A-Z0-9]{10})/i);
    const mainAsin  = product.asin || asinMatch?.[1];
    if (!groupSku || !mainAsin) { result.status = 'skipped_no_sku'; return result; }

    console.log(`[Worker] "${(product.title||'').slice(0,50)}" asin=${mainAsin}`);

    // ── 1. Scrape main ASIN page ───────────────────────────────────────────
    const mainProduct = await scrapeAsin(mainAsin);
    if (!mainProduct) {
      result.status = 'scrape_failed';
      await db.addLog('error', `Scrape failed: ${(product.title||'').slice(0,50)}`, 'Amazon blocked — will retry', { productId: product.id });
      return result;
    }

    const freshImages  = mainProduct.images || [];
    const mainRawPrice = mainProduct.price || 0;
    const freshStock   = mainProduct.inStock !== false;
    const defaultQty   = product.quantity || 1;
    const newQty       = freshStock ? defaultQty : 0;

    console.log(`[Worker]   imgs=${freshImages.length} price=$${mainRawPrice} inStock=${freshStock}`);

    // ── 2. Check variation or simple ─────────────────────────────────────
    const groupRes    = await getInventoryGroup(token, groupSku);
    const variantSkus = groupRes?.variantSKUs || [];
    const isVariation = variantSkus.length > 0;

    if (isVariation) {
      // ── VARIATION: update group images, then per-variant price+qty ──────

      // Update group image gallery (one call)
      if (freshImages.length && groupRes) {
        const gPut = { ...groupRes, imageUrls: freshImages };
        delete gPut.inventoryItemGroupKey;
        const gR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`,
          { method: 'PUT', headers: ebayAuthLang(token), body: JSON.stringify(gPut) });
        if (gR.ok) console.log(`[Worker]   group images: ${freshImages.length}`);
      }

      // Build color→ASIN map from scraped product
      const colorAsinMap = mainProduct.colorAsinMap || {};
      // Also try from variations array
      const colorGroup = (mainProduct.variations||[]).find(v => /color|colour/i.test(v.name));
      if (colorGroup?.values?.length) {
        for (const v of colorGroup.values) {
          if (v.asin && !colorAsinMap[v.value]) colorAsinMap[v.value] = v.asin;
        }
      }

      console.log(`[Worker]   colorAsinMap: ${Object.keys(colorAsinMap).length} colors`);

      // Pre-fetch all color ASIN prices (batched 3 at a time)
      const priceCache = {}; // color → { price, inStock }
      const colorEntries = Object.entries(colorAsinMap);
      if (colorEntries.length > 0) {
        for (let i = 0; i < colorEntries.length; i += 3) {
          const batch = colorEntries.slice(i, i + 3);
          await Promise.all(batch.map(async ([color, asin]) => {
            const p = await scrapeAsin(asin);
            if (p && p.price > 0) {
              priceCache[color] = { price: p.price, inStock: p.inStock !== false };
              console.log(`[Worker]   ${color}: $${p.price} ${p.inStock!==false?'✓':'OOS'}`);
            } else {
              priceCache[color] = { price: mainRawPrice, inStock: freshStock };
            }
          }));
          if (i + 3 < colorEntries.length) await sleep(2000);
        }
      }

      // Update all variant SKUs
      const BATCH = 4;
      for (let i = 0; i < variantSkus.length; i += BATCH) {
        const batch = variantSkus.slice(i, i + BATCH);
        await Promise.all(batch.map(async (sku) => {
          try {
            const suffix   = parseSkuSuffix(sku, groupSku);
            const color    = matchColor(suffix, colorAsinMap);
            const colorData = color ? priceCache[color] : null;
            const rawPrice = colorData?.price || mainRawPrice;
            const varStock = colorData?.inStock ?? freshStock;
            const varQty   = varStock ? defaultQty : 0;
            const newPrice = rawPrice > 0 ? applyMarkup(rawPrice, markup) : 0;

            // GET current item
            const cur    = await getInventoryItem(token, sku);
            const oldQty = cur?.availability?.shipToLocationAvailability?.quantity ?? -1;

            // PUT qty (images are at group level for variations)
            const putBody = {
              availability: { shipToLocationAvailability: { quantity: varQty } },
              condition: cur?.condition || 'NEW',
              product: cur?.product || {},
            };
            if (cur?.packageWeightAndSize) putBody.packageWeightAndSize = cur.packageWeightAndSize;

            const putOk = await putInventoryItem(token, sku, putBody);
            if (putOk && oldQty >= 0 && oldQty !== varQty) {
              result.stockChanges.push(varStock ? `Restocked (${varQty})` : 'OOS');
              if (!varStock) result.wentOos = true;
            }

            // PUT offer price
            if (newPrice > 0) {
              const offer = await getOffer(token, sku);
              if (offer?.offerId) {
                const oldPrice = parseFloat(offer.pricingSummary?.price?.value || 0);
                if (Math.abs(newPrice - oldPrice) > 0.01) {
                  const ok = await putOfferPrice(token, offer.offerId, newPrice);
                  if (ok) {
                    const label = color || suffix.slice(0,12);
                    result.priceChanges.push(`${label}: $${oldPrice.toFixed(2)}→$${newPrice.toFixed(2)}`);
                    console.log(`[Worker]   ${sku.slice(-20)}: $${oldPrice.toFixed(2)}→$${newPrice.toFixed(2)}`);
                  }
                }
              }
            }
          } catch(e) { console.warn(`[Worker]   variant err ${sku.slice(-15)}:`, e.message); }
        }));
        if (i + BATCH < variantSkus.length) await sleep(500);
      }

      const mainNewPrice = mainRawPrice > 0 ? applyMarkup(mainRawPrice, markup) : 0;
      await db.updateProductSync(product.id, { myPrice: mainNewPrice, amazonPrice: mainRawPrice, lastSynced: new Date().toISOString(), status: product.status, quantity: newQty });

      if (result.priceChanges.length) await db.addLog('price', `Prices updated: ${(product.title||'').slice(0,50)}`, result.priceChanges.slice(0,8).join(' · '), { productId: product.id });
      if (result.stockChanges.length) await db.addLog('stock', `Stock updated: ${(product.title||'').slice(0,50)}`, [...new Set(result.stockChanges)].join(' · '), { productId: product.id });
      if (!result.priceChanges.length && !result.stockChanges.length) {
        await db.addLog('sync', `Monitored: ${(product.title||'').slice(0,50)}`, `${variantSkus.length} variants · no changes`, { productId: product.id });
      }

    } else {
      // ── SIMPLE LISTING ──────────────────────────────────────────────────
      const newPrice = mainRawPrice > 0 ? applyMarkup(mainRawPrice, markup) : 0;

      const cur    = await getInventoryItem(token, groupSku);
      const oldQty = cur?.availability?.shipToLocationAvailability?.quantity ?? -1;

      const putBody = {
        availability: { shipToLocationAvailability: { quantity: newQty } },
        condition: cur?.condition || 'NEW',
        product: { ...(cur?.product || {}), imageUrls: freshImages },
      };
      if (cur?.packageWeightAndSize) putBody.packageWeightAndSize = cur.packageWeightAndSize;
      await putInventoryItem(token, groupSku, putBody);

      if (oldQty >= 0 && oldQty !== newQty) {
        result.stockChanges.push(freshStock ? `Restocked (${newQty})` : 'OOS');
        if (!freshStock) result.wentOos = true;
      }

      if (newPrice > 0) {
        const offer = await getOffer(token, groupSku);
        if (offer?.offerId) {
          const oldPrice = parseFloat(offer.pricingSummary?.price?.value || 0);
          if (Math.abs(newPrice - oldPrice) > 0.01) {
            const ok = await putOfferPrice(token, offer.offerId, newPrice);
            if (ok) {
              result.priceChanges.push(`$${oldPrice.toFixed(2)} → $${newPrice.toFixed(2)}`);
              console.log(`[Worker]   price: $${oldPrice.toFixed(2)} → $${newPrice.toFixed(2)}`);
            }
          }
        }
      }

      await db.updateProductSync(product.id, { myPrice: newPrice, amazonPrice: mainRawPrice, lastSynced: new Date().toISOString(), status: product.status, quantity: newQty });

      if (result.priceChanges.length) await db.addLog('price', `Price updated: ${(product.title||'').slice(0,50)}`, result.priceChanges.join(' · '), { productId: product.id });
      if (result.stockChanges.length) await db.addLog('stock', `Stock updated: ${(product.title||'').slice(0,50)}`, result.stockChanges.join(' · '), { productId: product.id });
      if (!result.priceChanges.length && !result.stockChanges.length) {
        await db.addLog('sync', `Monitored: ${(product.title||'').slice(0,50)}`, `No changes · $${newPrice.toFixed(2)} · ${freshImages.length} imgs`, { productId: product.id });
      }
    }

    // Webhooks
    if (webhookUrl) {
      if (result.priceChanges.length) await sendWebhook(webhookUrl, 'price', product, result.priceChanges.slice(0,10));
      if (result.stockChanges.length) await sendWebhook(webhookUrl, 'stock', product, [...new Set(result.stockChanges)]);
    }

  } catch(e) {
    result.status = 'error';
    result.error  = e.message;
    console.error(`[Worker] syncProduct error ${product.id}:`, e.message);
    await db.addLog('error', `Sync error: ${(product.title||'').slice(0,50)}`, e.message, { productId: product.id });
  }

  return result;
}

// ── Main sync job ─────────────────────────────────────────────────────────────
async function runSync() {
  if (isSyncing) { console.log('[Worker] Already running, skipping'); return; }
  isSyncing = true;
  console.log('[Worker] === Sync cycle starting ===');

  try {
    const token = await getValidToken();
    if (!token) {
      console.log('[Worker] No valid token');
      await db.addLog('error', 'Sync skipped', 'No valid eBay token — re-connect in Settings', {});
      return;
    }

    const markup     = parseFloat(await db.getSetting('markup') || 23);
    const webhookUrl = await db.getSetting('webhookUrl') || null;
    const products   = await db.getProductsForSync(50);

    if (!products.length) { console.log('[Worker] No products to sync'); return; }
    console.log(`[Worker] ${products.length} products · markup=${markup}%`);

    const totals = { priceChanges: 0, stockChanges: 0, oos: 0, errors: 0, skipped: 0 };

    for (const p of products) {
      if (!p.ebaySku || p.ebaySku.startsWith('DS-RECOVERED') || p.ebaySku.startsWith('DS-EXISTING')) {
        totals.skipped++; continue;
      }
      const r = await syncProduct(p, token, markup, webhookUrl);
      totals.priceChanges += r.priceChanges?.length || 0;
      totals.stockChanges += r.stockChanges?.length || 0;
      if (r.wentOos) totals.oos++;
      if (r.status !== 'ok') {
        if (r.status.startsWith('skipped')) totals.skipped++;
        else totals.errors++;
      }
      await sleep(1500); // pace between products
    }

    const summary = `price:${totals.priceChanges} stock:${totals.stockChanges} oos:${totals.oos} errors:${totals.errors} skipped:${totals.skipped}`;
    console.log(`[Worker] === Done — ${summary} ===`);
    await db.setSetting('last_sync_run', new Date().toISOString());
    await db.setSetting('last_sync_summary', totals);
    await db.addLog('sync', 'Auto-sync complete', summary, {});

  } catch(e) {
    console.error('[Worker] Fatal error:', e.message);
    await db.addLog('error', 'Auto-sync fatal', e.message, {}).catch(()=>{});
  } finally {
    isSyncing = false;
  }
}

// ── Start ─────────────────────────────────────────────────────────────────────
function startWorker() {
  console.log('[Worker] Starting — syncs every 15 minutes');
  setTimeout(runSync, 45000);           // first run after 45s startup
  cron.schedule('*/15 * * * *', runSync); // then every 15 min
}

module.exports = { startWorker, runSync, getValidToken };
