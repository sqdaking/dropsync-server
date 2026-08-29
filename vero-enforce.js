// vero-enforce.js — authoritative detection of eBay policy removals
//
// WHY THIS REPLACES MESSAGE PARSING
// The previous approach read eBay Messages and guessed which listing a notice
// referred to. It missed removals whose notice contained no item ID, it could
// not see removals that never generated a message, and it depended on wording.
//
// eBay exposes the truth directly:
//   Item.SellingStatus.AdminEnded        → true when eBay ended the listing
//   Item.ItemPolicyViolation.PolicyID    → which policy was violated
//   Item.ItemPolicyViolation.PolicyText  → the human-readable reason
//   Item.SellingStatus.ListingOnHold     → listing suspended but not ended
//
// GetSellerList returns these in bulk, so one paginated sweep gives a complete,
// unambiguous list of every listing eBay has acted on — with the reason.
//
// WHAT IT DOES WITH THAT
//   1. Flags every affected listing do_not_relist (sync and push both honour it)
//   2. Removes it from the sync queue so nothing republishes it
//   3. Extracts the brand from the violation text and blocks that brand from
//      future imports — the same rights owner rarely reports only once
//   4. Records everything for review

const vero = require('./vero');

let _pool = null;
let _getUrls = null;

async function initEnforce(pool, getEbayUrls) {
  _pool = pool;
  _getUrls = getEbayUrls;
  await _pool.query(`
    CREATE TABLE IF NOT EXISTS policy_violations (
      item_id      TEXT PRIMARY KEY,
      account_id   TEXT NOT NULL DEFAULT 'default',
      ebay_sku     TEXT,
      title        TEXT,
      policy_id    TEXT,
      policy_text  TEXT,
      kind         TEXT,               -- admin_ended | on_hold
      brand        TEXT,
      ended_at     TIMESTAMPTZ,
      flagged      BOOLEAN NOT NULL DEFAULT FALSE,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS policy_violations_acct_idx ON policy_violations(account_id, created_at DESC);
    -- PERMANENT IMPORT BLOCKLIST.
    -- do_not_relist protects an existing eBay listing, but the SOURCE product
    -- could still be re-imported from Amazon and pushed as a brand-new listing.
    -- Four products in the Seller Help report were flagged twice for exactly
    -- this reason. Blocking the ASIN — and a title fingerprint, since the same
    -- product reappears under different ASINs — closes that loop.
    CREATE TABLE IF NOT EXISTS blocked_products (
      key         TEXT PRIMARY KEY,      -- 'asin:B0XXXX' or 'fp:<fingerprint>'
      kind        TEXT NOT NULL,         -- asin | fingerprint
      asin        TEXT,
      title       TEXT,
      reason      TEXT,
      item_id     TEXT,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `).catch(e => console.warn('[enforce] schema:', e.message));
  console.log('[enforce] policy-violation scanner ready');
}

function xmlEsc(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

async function trading(callName, token, innerXml) {
  const r = await fetch(_getUrls().EBAY_TRADING, {
    method: 'POST',
    headers: {
      'X-EBAY-API-COMPATIBILITY-LEVEL': '1193',
      'X-EBAY-API-CALL-NAME': callName,
      'X-EBAY-API-SITEID': '0',
      'X-EBAY-API-IAF-TOKEN': token,
      'Content-Type': 'text/xml',
    },
    body: `<?xml version="1.0" encoding="utf-8"?>
<${callName}Request xmlns="urn:ebay:apis:eBLBaseComponents">${innerXml}</${callName}Request>`,
  });
  return r.text();
}

const tag = (block, name) => {
  const m = block.match(new RegExp(`<${name}>([\\s\\S]*?)</${name}>`));
  return m ? m[1].trim() : '';
};

// The same product reappears under different ASINs and slightly different
// titles, so an exact fingerprint is too brittle — it failed to connect
// "Tankini Swimsuits for Women, Two Piece Athletic" with "2025 Tankini
// Swimsuits for Women, Two Piece Bathing Suits", which are the same item and
// were both reported. Compare word SETS instead and treat a 60% overlap as the
// same product.
const _STOP = new Set(['for','the','and','with','two','set','pack','new','all','size','inch','inches','from','you','your','use','one']);
function titleWords(title) {
  return [...new Set(String(title || '').toLowerCase()
    .replace(/[^a-z ]/g, ' ').split(/\s+/)
    .filter(w => w.length > 2 && !_STOP.has(w)))];
}
function similarity(a, b) {
  if (!a.length || !b.length) return 0;
  const A = new Set(a), B = new Set(b);
  let inter = 0;
  for (const w of A) if (B.has(w)) inter++;
  return inter / (A.size + B.size - inter);      // Jaccard
}
function fingerprint(title) {
  return titleWords(title).sort().slice(0, 8).join('-');
}

// End a listing outright. eBay has usually ended it already, in which case this
// is a harmless no-op — but for listings merely HIDDEN (not ended) it is the
// difference between the item being gone and quietly coming back.
async function endListing(token, itemId) {
  if (!itemId) return { ok: false, reason: 'no itemId' };
  try {
    const xml = await trading('EndFixedPriceItem', token,
      `<ItemID>${itemId}</ItemID><EndingReason>NotAvailable</EndingReason>`);
    if (/<Ack>Success<\/Ack>|<Ack>Warning<\/Ack>/.test(xml)) return { ok: true };
    const err = (xml.match(/<LongMessage>([\s\S]*?)<\/LongMessage>/) || [])[1] || '';
    // "Auction already closed" / "not found" means it is already gone.
    if (/already|ended|not found|invalid item/i.test(err)) return { ok: true, alreadyEnded: true };
    return { ok: false, reason: stripHtml(err).slice(0, 120) };
  } catch (e) { return { ok: false, reason: e.message }; }
}

// Never import this product again, by ASIN and by title fingerprint.
async function blockProduct({ asin, title, reason, itemId }) {
  if (!_pool) return;
  const rows = [];
  if (asin && /^[A-Z0-9]{10}$/.test(asin)) rows.push([`asin:${asin}`, 'asin', asin, title, reason, itemId]);
  const fp = fingerprint(title);
  if (fp && fp.length > 8) rows.push([`fp:${fp}`, 'fingerprint', asin || null, title, reason, itemId]);
  for (const r of rows) {
    await _pool.query(
      `INSERT INTO blocked_products(key, kind, asin, title, reason, item_id)
       VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(key) DO NOTHING`, r).catch(() => {});
  }
}

function stripHtml(s) {
  return String(s || '').replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/\s+/g, ' ').trim();
}

// Pull a brand out of the violation text or the listing title. The rights owner
// that reported once will report again, so blocking the brand matters more than
// blocking the single listing.
function brandFrom(policyText, title) {
  const hay = `${policyText || ''} ${title || ''}`;
  const known = [...vero.TIER_CRITICAL, ...vero.TIER_HIGH, ...vero.TIER_MEDIUM];
  for (const b of known) {
    const re = new RegExp(`(^|[^a-z0-9])${b.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}([^a-z0-9]|$)`, 'i');
    if (re.test(hay)) return b;
  }
  const m = (policyText || '').match(/rights?\s+owner[:\s]+([^.,;\n]{2,40})/i);
  if (m) return m[1].trim().toLowerCase();
  return null;
}

/**
 * Sweep the seller's listings for anything eBay ended or put on hold.
 * @param {object} o { token, accountId, days, autoFlag }
 */
async function scanPolicyViolations({ token, accountId = 'default', days = 60, autoFlag = true }) {
  if (!token) throw new Error('access_token required');
  const end = new Date();
  const start = new Date(Date.now() - Math.min(120, Math.max(1, days)) * 86400000);

  const found = [];
  // GetSellerList returns ended listings in the window, including the ones eBay
  // ended itself — which is exactly what we're after.
  for (let page = 1; page <= 25; page++) {
    const xml = await trading('GetSellerList', token,
      `<EndTimeFrom>${start.toISOString()}</EndTimeFrom>` +
      `<EndTimeTo>${end.toISOString()}</EndTimeTo>` +
      `<IncludeVariations>false</IncludeVariations>` +
      `<GranularityLevel>Medium</GranularityLevel>` +
      `<DetailLevel>ReturnAll</DetailLevel>` +
      `<Pagination><EntriesPerPage>200</EntriesPerPage><PageNumber>${page}</PageNumber></Pagination>`);
    if (/<Ack>Failure<\/Ack>/.test(xml)) {
      const err = (xml.match(/<LongMessage>([\s\S]*?)<\/LongMessage>/) || [])[1] || 'unknown';
      throw new Error('GetSellerList failed: ' + stripHtml(err).slice(0, 200));
    }
    const items = xml.match(/<Item>[\s\S]*?<\/Item>/g) || [];
    for (const it of items) {
      const adminEnded = /<AdminEnded>true<\/AdminEnded>/i.test(it);
      const onHold     = /<ListingOnHold>true<\/ListingOnHold>/i.test(it);
      const hasViol    = /<ItemPolicyViolation>/i.test(it);
      if (!adminEnded && !onHold && !hasViol) continue;
      const violBlock = (it.match(/<ItemPolicyViolation>[\s\S]*?<\/ItemPolicyViolation>/) || [''])[0];
      found.push({
        itemId:     tag(it, 'ItemID'),
        title:      stripHtml(tag(it, 'Title')),
        sku:        tag(it, 'SKU'),
        policyId:   tag(violBlock, 'PolicyID'),
        policyText: stripHtml(tag(violBlock, 'PolicyText')),
        kind:       adminEnded ? 'admin_ended' : (onHold ? 'on_hold' : 'violation'),
        endedAt:    tag(it, 'EndTime'),
      });
    }
    const total = parseInt((xml.match(/<TotalNumberOfPages>(\d+)<\/TotalNumberOfPages>/) || [])[1] || '1');
    if (page >= total) break;
    await new Promise(r => setTimeout(r, 300));
  }

  let flagged = 0;
  const brandsBlocked = new Set();
  for (const f of found) {
    const brand = brandFrom(f.policyText, f.title);
    if (brand) brandsBlocked.add(brand);

    if (_pool) {
      // Match by eBay item ID, then by SKU as a fallback.
      let rows = [];
      if (f.itemId) {
        const r = await _pool.query(
          `SELECT id, ebay_sku, asin FROM products WHERE account_id=$1 AND ebay_item_id=$2`,
          [accountId, f.itemId]).catch(() => ({ rows: [] }));
        rows = r.rows;
      }
      if (!rows.length && f.sku) {
        const r = await _pool.query(
          `SELECT id, ebay_sku, asin FROM products WHERE account_id=$1 AND ebay_sku=$2`,
          [accountId, f.sku]).catch(() => ({ rows: [] }));
        rows = r.rows;
      }
      // END the listing outright, not just flag it. A flagged-but-live listing
      // keeps selling, and a HIDDEN listing quietly returns when the hold
      // lifts. Set AUTO_END_ON_VIOLATION=off to flag only.
      if (String(process.env.AUTO_END_ON_VIOLATION || 'on').toLowerCase() !== 'off' && f.itemId) {
        const e = await endListing(token, f.itemId);
        console.log(`[enforce] ended listing ${f.itemId}: ${e.ok ? (e.alreadyEnded ? 'already ended' : 'ENDED') : 'failed — ' + e.reason}`);
      }
      // Permanently block the source product from being re-imported.
      {
        const asin = rows[0]?.asin || (f.sku || '').match(/[A-Z0-9]{10}/)?.[0] || null;
        await blockProduct({ asin, title: f.title,
                             reason: `${f.kind}: ${(f.policyText || f.policyId || '').slice(0, 120)}`,
                             itemId: f.itemId });
      }
      if (autoFlag && rows.length) {
        const ids = rows.map(x => x.id);
        const up = await _pool.query(
          `UPDATE products SET do_not_relist=TRUE, vero_note=$2, updated_at=NOW() WHERE id = ANY($1)`,
          [ids, `eBay ${f.kind}: ${(f.policyText || f.policyId || '').slice(0, 160)}`]
        ).catch(() => ({ rowCount: 0 }));
        flagged += up.rowCount || 0;
        // Remove from the sync queue so nothing republishes it.
        await _pool.query(
          `DELETE FROM relay_state WHERE ebay_sku IN (
             SELECT ebay_sku FROM products WHERE id = ANY($1) AND ebay_sku IS NOT NULL)`,
          [ids]).catch(() => {});
      }
      await _pool.query(
        `INSERT INTO policy_violations(item_id, account_id, ebay_sku, title, policy_id, policy_text, kind, brand, ended_at, flagged)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
         ON CONFLICT(item_id) DO UPDATE SET flagged=EXCLUDED.flagged, policy_text=EXCLUDED.policy_text`,
        [f.itemId || `noid-${Date.now()}-${Math.random()}`, accountId, f.sku || rows[0]?.ebay_sku || null,
         f.title, f.policyId, f.policyText, f.kind, brand,
         f.endedAt ? new Date(f.endedAt) : null, rows.length > 0]).catch(() => {});
    }

    // Block the brand from future imports — the same owner reports repeatedly.
    if (brand && _pool) {
      await _pool.query(
        `INSERT INTO vero_brands(brand, tier) VALUES($1,'critical') ON CONFLICT(brand) DO NOTHING`,
        [brand]).catch(() => {});
    }
  }

  console.log(`[enforce] ${accountId}: ${found.length} policy action(s) found, ${flagged} listing(s) flagged, brands blocked: ${[...brandsBlocked].join(', ') || 'none'}`);
  return {
    found: found.length,
    flagged,
    brandsBlocked: [...brandsBlocked],
    items: found.slice(0, 100),
  };
}

function mountEnforce(app) {
  const acct = req => {
    const a = String(req.query.account || '').trim();
    return /^[\w.\-]{1,64}$/.test(a) ? a : 'default';
  };

  app.post('/api/vero/scan-violations', async (req, res) => {
    try {
      const { access_token, days = 60, autoFlag = true } = req.body || {};
      if (!access_token) return res.status(400).json({ error: 'access_token required' });
      let accountId = acct(req);
      try {
        const real = await require('./ebay').resolveAccountId(access_token);
        if (real && real !== 'default') accountId = real;
      } catch (e) {}
      const out = await scanPolicyViolations({ token: access_token, accountId, days, autoFlag });
      res.json({ success: true, accountId, ...out });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/api/vero/violations', async (req, res) => {
    try {
      const r = await _pool.query(
        `SELECT item_id, ebay_sku, title, policy_id, policy_text, kind, brand, ended_at, flagged
           FROM policy_violations WHERE account_id=$1 ORDER BY ended_at DESC NULLS LAST LIMIT 300`,
        [acct(req)]);
      res.json({ violations: r.rows });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  console.log('[enforce] routes mounted: /api/vero/scan-violations, /api/vero/violations');
}

// Is this product permanently blocked from import?
async function isProductBlocked({ asin, title }) {
  if (!_pool) return null;
  const keys = [];
  if (asin && /^[A-Z0-9]{10}$/.test(asin)) keys.push(`asin:${asin}`);
  const fp = fingerprint(title);
  if (fp && fp.length > 8) keys.push(`fp:${fp}`);
  if (!keys.length) return null;
  try {
    // Exact ASIN or fingerprint hit
    const r = await _pool.query(
      `SELECT key, kind, title, reason FROM blocked_products WHERE key = ANY($1) LIMIT 1`, [keys]);
    if (r.rows[0]) return r.rows[0];
    // Near-duplicate title: same product, different ASIN or reworded title.
    const words = titleWords(title);
    if (words.length >= 3) {
      const all = await _pool.query(
        `SELECT key, kind, title, reason FROM blocked_products WHERE title IS NOT NULL LIMIT 2000`);
      for (const row of all.rows) {
        if (similarity(words, titleWords(row.title)) >= 0.6) {
          return { ...row, kind: 'similar title', matched: row.title };
        }
      }
    }
    return null;
  } catch (e) { return null; }
}

module.exports = { initEnforce, mountEnforce, scanPolicyViolations, brandFrom,
                   endListing, blockProduct, isProductBlocked, fingerprint };
