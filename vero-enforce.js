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
          `SELECT id, ebay_sku FROM products WHERE account_id=$1 AND ebay_item_id=$2`,
          [accountId, f.itemId]).catch(() => ({ rows: [] }));
        rows = r.rows;
      }
      if (!rows.length && f.sku) {
        const r = await _pool.query(
          `SELECT id, ebay_sku FROM products WHERE account_id=$1 AND ebay_sku=$2`,
          [accountId, f.sku]).catch(() => ({ rows: [] }));
        rows = r.rows;
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

module.exports = { initEnforce, mountEnforce, scanPolicyViolations, brandFrom };
