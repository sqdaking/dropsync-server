// shopify.js — Shopify destination adapter (Amazon → Shopify, selling into Morocco)
//
// DELIBERATELY SEPARATE FROM ebay.js
// Nothing here touches eBay state: different tables, different settings, its own
// pricing. The only thing shared is the Amazon side — the extension, the price
// and stock extraction, the variant mapping — which is where the hard work is.
// eBay syncing behaves exactly as before whether or not this module is enabled.
//
// GRAPHQL, NOT REST
// Shopify's REST /products and /variants endpoints were deprecated in 2024-04
// and custom apps must use the GraphQL Admin API. REST also refuses to write to
// products with more than 100 variants, which this catalogue would hit.
//
// LANDED COST
// An eBay listing prices Amazon cost + markup. Selling into Morocco has to
// carry the whole chain or the margin is imaginary:
//
//   Amazon price
//   + US shipping to the forwarder
//   + international freight (weight-based)
//   = CIF value
//   + customs duty   (% of CIF, varies by product category)
//   + VAT            (20% in Morocco, charged on CIF + duty)
//   + handling
//   × (1 + markup)
//   × USD→MAD
//   → rounded to a sensible retail price
//
// Every input is configurable per account and overridable per product, because
// duty rates differ by category and freight differs by weight.

const API_VERSION = '2025-01';

let _pool = null;

async function initShopify(pool) {
  _pool = pool;
  await _pool.query(`
    CREATE TABLE IF NOT EXISTS shopify_settings (
      account_id TEXT NOT NULL,
      key        TEXT NOT NULL,
      value      TEXT,
      PRIMARY KEY (account_id, key)
    );
    CREATE TABLE IF NOT EXISTS shopify_products (
      id             SERIAL PRIMARY KEY,
      account_id     TEXT NOT NULL DEFAULT 'default',
      asin           TEXT NOT NULL,
      shopify_gid    TEXT,
      handle         TEXT,
      title          TEXT,
      variant_map    JSONB NOT NULL DEFAULT '{}'::jsonb,   -- variantGid → asin
      inventory_map  JSONB NOT NULL DEFAULT '{}'::jsonb,   -- variantGid → inventoryItemGid
      last_synced    TIMESTAMPTZ,
      status         TEXT NOT NULL DEFAULT 'active',
      created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (account_id, asin)
    );
  `).catch(e => console.warn('[shopify] schema:', e.message));
  console.log('[shopify] adapter ready (GraphQL Admin API ' + API_VERSION + ')');
}

// ── settings ────────────────────────────────────────────────────────────────
const DEFAULTS = {
  shopDomain: '',            // your-store.myshopify.com
  adminToken: '',            // custom app Admin API access token
  locationGid: '',           // inventory location; auto-detected on first push
  currency: 'MAD',
  fxUsdToMad: '10.0',        // review regularly; a stale rate silently erodes margin
  shipUsPerOrder: '0',       // Amazon → your US forwarder
  freightPerKg: '12',        // USD per kg, US → Morocco
  freightMinKg: '0.5',       // volumetric floor
  dutyPct: '25',             // Morocco customs duty, % of CIF (varies by category)
  vatPct: '20',              // Morocco VAT, on CIF + duty
  handlingMad: '30',         // your per-order handling
  markupPct: '35',
  digitalMarkupPct: '12',    // codes are compared instantly; margins are thinner
  codFeeMad: '60',           // surcharge for cash on delivery
  codEnabled: 'true',        // physical goods only — a courier cannot collect cash for an emailed code
  codRefusalPct: '25',       // share of COD orders refused at the door
  // A refusal is NOT a write-off. The parcel comes back to you and the item is
  // resaleable, and Amazon accepts returns for valid reasons — so the real cost
  // is the return leg plus whatever value is not recovered, not the goods.
  // That makes COD viable at much higher values than a total-loss model implies.
  // Refused parcels are liquidated quickly on a local marketplace (Avito,
  // Facebook Marketplace) rather than held for full price. A small loss taken
  // this week is cheaper than capital locked in a box for a month — stock that
  // does not move is stock that cannot buy the next order.
  liquidationPct: '80',      // % of landed cost you expect on a quick local sale
  codReturnCostMad: '60',    // courier return leg on a refused parcel
  // Delivery is TWO legs and the customer only cares about the total:
  //   Amazon → your US forwarder   (Amazon's own per-product estimate)
  // + forwarder → Morocco          (your freight carrier)
  // + local courier if COD
  // Showing Amazon's figure alone would promise days for a parcel that takes
  // weeks, and on COD an over-promise means a refused parcel you pay for twice.
  freightCarrier: 'DHL Express',
  freightDaysMin: '4',
  freightDaysMax: '7',
  codExtraDaysMin: '2',
  codExtraDaysMax: '4',
  // Off by default: with recoverable refusals the economics hold at high values
  // too. Set a figure here only if you want a ceiling for cash-flow reasons —
  // a refused 6,600 MAD parcel still ties up money until it resells.
  codMaxValueMad: '0',
  roundTo: '9',              // price ending, e.g. 249 → 249, 251 → 259
  minMarginMad: '40',        // refuse to list below this absolute margin
};

async function settings(accountId = 'default') {
  const out = { ...DEFAULTS };
  if (!_pool) return out;
  try {
    const r = await _pool.query(
      `SELECT key, value FROM shopify_settings WHERE account_id = $1`, [accountId]);
    for (const row of r.rows) out[row.key] = row.value;
  } catch (e) {}
  return out;
}

/**
 * Landed cost → retail price in MAD.
 * Returns every intermediate figure so a price can be explained rather than
 * just asserted — the same reason smartSync logs its pricing inputs.
 */
/**
 * Total delivery window shown to the customer: Amazon's own estimate for that
 * product plus the international leg, and the local courier leg on COD.
 * Returns null when Amazon gave us nothing, so the page can fall back to a
 * generic range rather than inventing a date.
 */
function deliveryWindow(amazonEtaDays, cfg, opts) {
  const n = (x, d = 0) => { const v = parseFloat(x); return Number.isFinite(v) ? v : d; };
  if (opts && opts.digital) {
    return { min: 0, max: 0, instant: true, carrier: null,
             text: { fr: 'Code envoyé aussitôt', ar: 'يصلك الكود فوراً', en: 'Code sent instantly' } };
  }
  if (amazonEtaDays == null) return null;
  const base = Math.max(0, Math.round(amazonEtaDays));
  let min = base + n(cfg.freightDaysMin, 4);
  let max = base + n(cfg.freightDaysMax, 7);
  if (opts && opts.cod) { min += n(cfg.codExtraDaysMin, 2); max += n(cfg.codExtraDaysMax, 4); }
  const carrier = cfg.freightCarrier || 'DHL Express';
  return {
    min, max, instant: false, carrier,
    text: {
      fr: `Livraison ${min} à ${max} jours · ${carrier}`,
      ar: `التوصيل من ${min} إلى ${max} يوماً · ${carrier}`,
      en: `Delivery ${min}–${max} days · ${carrier}`,
    },
  };
}

function landedPrice(amazonUsd, weightKg, cfg, opts) {
  const n = (x, d = 0) => { const v = parseFloat(x); return Number.isFinite(v) ? v : d; };
  const goods    = n(amazonUsd);

  // DIGITAL GOODS (gift cards, PSN top-ups) have no freight, no customs and no
  // import VAT — they are delivered by email. Running them through the physical
  // landed-cost chain would add duty and shipping to a product that has
  // neither, pricing you out of a market where the buyer can compare instantly.
  if (opts && opts.digital) {
    const fxD = n(cfg.fxUsdToMad, 10);
    const costMad = goods * fxD;
    const withMk = costMad * (1 + n(cfg.digitalMarkupPct ?? cfg.markupPct) / 100);
    const endD = n(cfg.roundTo, 9);
    let pD = Math.ceil(withMk);
    if (endD > 0) { const r = pD % 10; pD = pD - r + endD; if (pD < withMk) pD += 10; }
    return {
      priceMad: pD, landedMad: +costMad.toFixed(2), marginMad: +(pD - costMad).toFixed(2),
      viable: (pD - costMad) >= n(cfg.minMarginMad),
      digital: true,
      breakdown: { goodsUsd: +goods.toFixed(2), usShipUsd: 0, freightUsd: 0, weightKg: 0,
                   cifUsd: +goods.toFixed(2), dutyUsd: 0, vatUsd: 0,
                   landedUsd: +goods.toFixed(2), fx: fxD },
    };
  }
  // IMPORT FEES DEPOSIT: when Amazon Global ships directly to Morocco it
  // collects duty and tax up front. That item is already duty-paid, so adding
  // our own duty and VAT on top would charge the customer twice and price the
  // listing out of the market. When Amazon quoted a deposit we use it INSTEAD
  // of our estimate.
  const amazonDuty = (opts && n(opts.importFeesUsd)) || 0;
  const kg       = Math.max(n(weightKg, 0.5), n(cfg.freightMinKg, 0.5));
  const usShip   = n(cfg.shipUsPerOrder);
  const freight  = kg * n(cfg.freightPerKg);
  const cif      = goods + usShip + freight;
  const duty     = amazonDuty > 0 ? amazonDuty : cif * (n(cfg.dutyPct) / 100);
  const vat      = amazonDuty > 0 ? 0 : (cif + duty) * (n(cfg.vatPct) / 100);
  const landedUsd = cif + duty + vat;
  const fx       = n(cfg.fxUsdToMad, 10);
  const landedMad = landedUsd * fx + n(cfg.handlingMad);
  const withMarkup = landedMad * (1 + n(cfg.markupPct) / 100);

  // Round up to the configured ending (…9 by default) — Moroccan retail
  // convention and it avoids odd-looking converted prices.
  const ending = n(cfg.roundTo, 9);
  let price = Math.ceil(withMarkup);
  if (ending > 0) {
    const rem = price % 10;
    price = price - rem + ending;
    if (price < withMarkup) price += 10;
  }
  const marginMad = price - landedMad;
  // COD is a separate price, not a discount off this one: the surcharge covers
  // the courier's collection fee and part of the refusal risk. Digital goods
  // never get a COD price — see the digital branch above.
  const codFee = n(cfg.codFeeMad, 60);
  const codPrice = price + codFee;
  const codCap = n(cfg.codMaxValueMad, 1500);
  const codAllowedByValue = codCap <= 0 || price <= codCap;
  // A refused COD parcel costs the goods plus both delivery legs, so the
  // expected margin is materially lower than the sticker margin. Surfacing it
  // stops a product looking profitable when it is not.
  const refusal = n(cfg.codRefusalPct, 25) / 100;
  const liqPct  = n(cfg.liquidationPct, 80) / 100;
  // What to list a returned unit at for a quick local sale, rounded to the same
  // ending as retail so it reads like a normal price rather than a distress one.
  let liqPrice = Math.ceil(landedMad * liqPct);
  const endingL = n(cfg.roundTo, 9);
  if (endingL > 0) { const r = liqPrice % 10; liqPrice = liqPrice - r + endingL; if (liqPrice > landedMad * liqPct + 9) liqPrice -= 10; }
  // Cost of a refusal = return leg + the gap between landed cost and what the
  // quick sale recovers.
  const refusalCost = n(cfg.codReturnCostMad, 60) + Math.max(0, landedMad - liqPrice);
  const codExpectedMargin = (1 - refusal) * (codPrice - landedMad) - refusal * refusalCost;
  return {
    priceMad: price,
    codAvailable: String(cfg.codEnabled) === 'true' && codAllowedByValue,
    codBlockedReason: codAllowedByValue ? null
      : `above the ${codCap} MAD cash-on-delivery limit — a refusal would cost more than the margin on several sales`,
    codPriceMad: codPrice,
    codFeeMad: codFee,
    codExpectedMarginMad: +codExpectedMargin.toFixed(2),
    codRefusalCostMad: +refusalCost.toFixed(2),
    // Operational answer to "a parcel came back — what do I sell it for?"
    liquidationPriceMad: liqPrice,
    liquidationLossMad: +(landedMad - liqPrice).toFixed(2),
    codViable: codExpectedMargin >= n(cfg.minMarginMad),
    landedMad: +landedMad.toFixed(2),
    marginMad: +marginMad.toFixed(2),
    viable: marginMad >= n(cfg.minMarginMad),
    breakdown: {
      goodsUsd: +goods.toFixed(2), usShipUsd: +usShip.toFixed(2),
      freightUsd: +freight.toFixed(2), weightKg: kg,
      cifUsd: +cif.toFixed(2), dutyUsd: +duty.toFixed(2), vatUsd: +vat.toFixed(2),
      landedUsd: +landedUsd.toFixed(2), fx,
      dutySource: amazonDuty > 0 ? 'amazon import fees deposit' : 'estimated',
    },
  };
}

// ── GraphQL ─────────────────────────────────────────────────────────────────
async function gql(cfg, query, variables) {
  if (!cfg.shopDomain || !cfg.adminToken) throw new Error('Shopify not configured (shopDomain / adminToken)');
  const r = await fetch(`https://${cfg.shopDomain}/admin/api/${API_VERSION}/graphql.json`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': cfg.adminToken },
    body: JSON.stringify({ query, variables }),
  });
  const d = await r.json().catch(() => ({}));
  if (d.errors) throw new Error('GraphQL: ' + JSON.stringify(d.errors).slice(0, 300));
  // Every Shopify mutation returns its own userErrors — surface them rather
  // than reporting a silent success.
  const root = d.data && Object.values(d.data)[0];
  const ue = root && root.userErrors;
  if (ue && ue.length) throw new Error('Shopify: ' + ue.map(e => `${(e.field||[]).join('.')} ${e.message}`).join('; ').slice(0, 300));
  return d.data;
}

async function defaultLocation(cfg) {
  if (cfg.locationGid) return cfg.locationGid;
  const d = await gql(cfg, `{ locations(first: 1) { nodes { id name } } }`);
  const gid = d?.locations?.nodes?.[0]?.id;
  if (!gid) throw new Error('no inventory location found on the store');
  return gid;
}

// ── create / update a product ───────────────────────────────────────────────
/**
 * productSet upserts the whole product — options, variants and all — in one
 * call, which is what the new product model is designed for. Creating a
 * product then adding variants one by one would be several times the calls and
 * can leave a half-built product behind on failure.
 */
async function pushProduct({ accountId = 'default', product, variants }) {
  const cfg = await settings(accountId);
  const loc = await defaultLocation(cfg);

  // SINGLE-VARIANT PRODUCTS.
  // Plenty of Amazon products have no variations at all, and plenty more are
  // worth listing as one item. Shopify's product model still requires at least
  // one option, so a single-variant product is expressed as the conventional
  // "Title / Default Title" pair — the same shape Shopify itself creates when
  // you add a product with no options in the admin.
  if (!Array.isArray(variants) || !variants.length) {
    variants = [{
      asin: product.asin,
      sku: product.sku || product.asin,
      amazonPrice: product.amazonPrice ?? product.price,
      weightKg: product.weightKg,
      inStock: product.inStock !== false,
      qty: product.qty,
      options: {},
    }];
  }
  const hasOptions = variants.some(v => Object.keys(v.options || {}).length > 0);
  const SINGLE_OPTION = 'Title', SINGLE_VALUE = 'Default Title';
  if (!hasOptions) {
    variants = variants.slice(0, 1).map(v => ({ ...v, options: { [SINGLE_OPTION]: SINGLE_VALUE } }));
  }

  const optionNames = [...new Set(variants.flatMap(v => Object.keys(v.options || {})))].slice(0, 3);
  const priced = [];
  for (const v of variants) {
    const p = landedPrice(v.amazonPrice, v.weightKg ?? product.weightKg, cfg,
                          { digital: product.digital === true,
                            importFeesUsd: v.importFeesUsd || product.importFeesUsd || 0 });
    if (!p.viable) {
      console.log(`[shopify] skipping variant ${v.sku}: margin ${p.marginMad} MAD below minimum`);
      continue;
    }
    priced.push({ ...v, price: p });
  }
  if (!priced.length) {
    const first = landedPrice(variants[0]?.amazonPrice, variants[0]?.weightKg ?? product.weightKg, cfg);
    throw new Error(`no variant clears the minimum margin (${cfg.minMarginMad} MAD). ` +
      `Example: $${first.breakdown.goodsUsd} lands at ${first.landedMad} MAD, ` +
      `retail ${first.priceMad}, margin ${first.marginMad}`);
  }

  const input = {
    title: product.title,
    descriptionHtml: product.description || '',
    vendor: product.brand || 'Import',
    status: 'ACTIVE',
    // Tags drive the storefront: 'digital' switches off COD and the delivery
    // promise, 'dept:*' routes the product to the right landing page.
    tags: product.tags || [],
    // Per-product delivery estimate, stored as metafields so the theme can show
    // it on the card. A blanket store-wide promise would be wrong for half the
    // catalogue — Amazon's own estimate varies from days to weeks by item.
    metafields: (function () {
      const w = deliveryWindow(product.etaDays, cfg, { digital: product.digital === true });
      const wc = deliveryWindow(product.etaDays, cfg, { cod: true });
      const out = [];
      if (w) {
        out.push({ namespace: 'custom', key: 'delivery_fr', type: 'single_line_text_field', value: w.text.fr });
        out.push({ namespace: 'custom', key: 'delivery_ar', type: 'single_line_text_field', value: w.text.ar });
        out.push({ namespace: 'custom', key: 'delivery_en', type: 'single_line_text_field', value: w.text.en });
        if (w.carrier) out.push({ namespace: 'custom', key: 'carrier', type: 'single_line_text_field', value: w.carrier });
      }
      if (wc && !wc.instant) {
        out.push({ namespace: 'custom', key: 'delivery_cod_fr', type: 'single_line_text_field',
                   value: `Paiement à la livraison : ${wc.min} à ${wc.max} jours` });
      }
      return out;
    })(),
    productOptions: optionNames.map(name => ({
      name,
      values: [...new Set(priced.map(v => v.options[name]).filter(Boolean))].map(v => ({ name: v })),
    })),
    variants: priced.map(v => ({
      optionValues: optionNames.map(name => ({ optionName: name, name: v.options[name] || 'Default' })),
      price: String(v.price.priceMad),
      sku: v.sku,
      inventoryItem: { tracked: true, measurement: { weight: { value: v.weightKg ?? 0.5, unit: 'KILOGRAMS' } } },
      inventoryQuantities: [{ locationId: loc, name: 'available', quantity: v.inStock ? (v.qty ?? 5) : 0 }],
    })),
    files: (product.images || []).slice(0, 10).map(src => ({ originalSource: src, contentType: 'IMAGE' })),
  };

  const d = await gql(cfg, `
    mutation Push($input: ProductSetInput!) {
      productSet(synchronous: true, input: $input) {
        product {
          id handle
          variants(first: 100) { nodes { id sku inventoryItem { id } } }
        }
        userErrors { field message }
      }
    }`, { input });

  const prod = d.productSet.product;
  const variantMap = {}, inventoryMap = {};
  for (const node of prod.variants.nodes) {
    const src = priced.find(v => v.sku === node.sku);
    if (src) { variantMap[node.id] = src.asin; inventoryMap[node.id] = node.inventoryItem?.id; }
  }
  if (_pool) {
    await _pool.query(
      `INSERT INTO shopify_products(account_id, asin, shopify_gid, handle, title, variant_map, inventory_map, last_synced)
       VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,NOW())
       ON CONFLICT (account_id, asin) DO UPDATE SET
         shopify_gid=EXCLUDED.shopify_gid, handle=EXCLUDED.handle, title=EXCLUDED.title,
         variant_map=EXCLUDED.variant_map, inventory_map=EXCLUDED.inventory_map, last_synced=NOW()`,
      [accountId, product.asin, prod.id, prod.handle, product.title,
       JSON.stringify(variantMap), JSON.stringify(inventoryMap)]).catch(() => {});
  }
  console.log(`[shopify] pushed "${String(product.title).slice(0,50)}" → ${prod.handle} (${priced.length} variant(s))`);
  return { gid: prod.id, handle: prod.handle, variants: priced.length,
           sample: priced[0]?.price };
}

// ── price + stock sync ──────────────────────────────────────────────────────
/**
 * Same discipline as the eBay sync: a variant with no fresh Amazon price is set
 * to zero stock rather than left selling on an unverified number.
 */
async function syncProduct({ accountId = 'default', asin, asinData }) {
  const cfg = await settings(accountId);
  if (!_pool) throw new Error('db unavailable');
  const r = await _pool.query(
    `SELECT shopify_gid, variant_map, inventory_map FROM shopify_products
      WHERE account_id=$1 AND asin=$2`, [accountId, asin]);
  if (!r.rows.length) return { skipped: true, reason: 'not on shopify' };
  const { shopify_gid, variant_map, inventory_map } = r.rows[0];
  const loc = await defaultLocation(cfg);

  const priceUpdates = [];
  const stockUpdates = [];
  let unpriced = 0;
  for (const [variantGid, vAsin] of Object.entries(variant_map || {})) {
    const d = asinData[vAsin];
    if (!d || !(d.price > 0)) {
      unpriced++;
      const inv = (inventory_map || {})[variantGid];
      if (inv) stockUpdates.push({ inventoryItemId: inv, locationId: loc, quantity: 0 });
      continue;
    }
    const p = landedPrice(d.price, d.weightKg, cfg, { digital: d.digital === true });
    priceUpdates.push({ id: variantGid, price: String(p.priceMad) });
    const inv = (inventory_map || {})[variantGid];
    if (inv) stockUpdates.push({ inventoryItemId: inv, locationId: loc,
                                 quantity: d.inStock && p.viable ? 5 : 0 });
  }

  if (priceUpdates.length) {
    await gql(cfg, `
      mutation Reprice($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
        productVariantsBulkUpdate(productId: $productId, variants: $variants) {
          productVariants { id price }
          userErrors { field message }
        }
      }`, { productId: shopify_gid, variants: priceUpdates });
  }
  if (stockUpdates.length) {
    await gql(cfg, `
      mutation SetStock($input: InventorySetQuantitiesInput!) {
        inventorySetQuantities(input: $input) {
          inventoryAdjustmentGroup { createdAt }
          userErrors { field message }
        }
      }`, { input: { name: 'available', ignoreCompareQuantity: true,
                     quantities: stockUpdates.map(s => ({ inventoryItemId: s.inventoryItemId,
                                                          locationId: s.locationId, quantity: s.quantity })) } });
  }
  await _pool.query(`UPDATE shopify_products SET last_synced=NOW() WHERE account_id=$1 AND asin=$2`,
    [accountId, asin]).catch(() => {});
  console.log(`[shopify] synced ${asin}: ${priceUpdates.length} repriced, ${stockUpdates.length} stock set` +
    (unpriced ? `, ${unpriced} unpriced → zeroed` : ''));
  return { repriced: priceUpdates.length, stockSet: stockUpdates.length, unpriced };
}

/** Unpublish rather than delete — reversible, and keeps the URL and any SEO. */
async function endProduct({ accountId = 'default', asin }) {
  const cfg = await settings(accountId);
  const r = await _pool.query(`SELECT shopify_gid FROM shopify_products WHERE account_id=$1 AND asin=$2`,
    [accountId, asin]);
  if (!r.rows.length) return { skipped: true };
  await gql(cfg, `
    mutation Unpublish($input: ProductInput!) {
      productUpdate(input: $input) { product { id status } userErrors { field message } }
    }`, { input: { id: r.rows[0].shopify_gid, status: 'DRAFT' } });
  await _pool.query(`UPDATE shopify_products SET status='draft' WHERE account_id=$1 AND asin=$2`,
    [accountId, asin]).catch(() => {});
  return { ok: true };
}

// ── routes ──────────────────────────────────────────────────────────────────
function mountShopify(app) {
  const acct = req => {
    const a = String(req.query.account || req.body?.accountId || '').trim();
    return /^[\w.\-]{1,64}$/.test(a) ? a : 'default';
  };

  app.get('/api/shopify/settings', async (req, res) => {
    const s = await settings(acct(req));
    res.json({ ...s, adminToken: s.adminToken ? '***set***' : '' });   // never echo the token
  });

  app.post('/api/shopify/settings', async (req, res) => {
    try {
      const a = acct(req);
      for (const [k, v] of Object.entries(req.body || {})) {
        if (k === 'accountId' || !(k in DEFAULTS)) continue;
        await _pool.query(
          `INSERT INTO shopify_settings(account_id,key,value) VALUES($1,$2,$3)
           ON CONFLICT (account_id,key) DO UPDATE SET value=EXCLUDED.value`, [a, k, String(v)]);
      }
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Price preview — check the maths on a real product before listing anything.
  app.post('/api/shopify/quote', async (req, res) => {
    try {
      const cfg = await settings(acct(req));
      const { amazonPrice, weightKg } = req.body || {};
      res.json(landedPrice(amazonPrice, weightKg, cfg));
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.post('/api/shopify/push', async (req, res) => {
    try {
      const { product, variants } = req.body || {};
      if (!product?.asin || !Array.isArray(variants)) return res.status(400).json({ error: 'product + variants required' });
      res.json({ success: true, ...(await pushProduct({ accountId: acct(req), product, variants })) });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.post('/api/shopify/sync', async (req, res) => {
    try {
      const { asin, asinData } = req.body || {};
      if (!asin || !asinData) return res.status(400).json({ error: 'asin + asinData required' });
      res.json({ success: true, ...(await syncProduct({ accountId: acct(req), asin, asinData })) });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.post('/api/shopify/end', async (req, res) => {
    try { res.json({ success: true, ...(await endProduct({ accountId: acct(req), asin: req.body?.asin })) }); }
    catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.get('/api/shopify/products', async (req, res) => {
    try {
      const r = await _pool.query(
        `SELECT asin, handle, title, status, last_synced,
                (SELECT count(*) FROM jsonb_object_keys(variant_map)) AS variants
           FROM shopify_products WHERE account_id=$1
          ORDER BY last_synced DESC NULLS LAST LIMIT 500`, [acct(req)]);
      res.json({ products: r.rows });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  console.log('[shopify] routes mounted: /api/shopify/{settings,quote,push,sync,end,products}');
}

module.exports = { initShopify, mountShopify, landedPrice, deliveryWindow, pushProduct, syncProduct, endProduct, settings };
