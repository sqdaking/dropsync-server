// vero-inbox.js — pull eBay policy/VeRO notices from My Messages and act on them
//
// WHY THIS WORKS THIS WAY
// eBay has no seller-facing API for "show me my VeRO violations" — the VeRO API
// (createVeroReport / getVeroReasonCodes) is built for rights OWNERS to file
// reports, not for sellers to read them. What sellers get is a message in My
// Messages ("please check your eBay messages"), which the Trading API call
// GetMyMessages can retrieve. Alerts arrive as flagged messages.
//
// FLOW
//   1. GetMyMessages DetailLevel=ReturnHeaders  → message IDs + subjects
//   2. keep only IP/policy-looking subjects
//   3. GetMyMessages DetailLevel=ReturnMessages (max 10 IDs per call) → body text
//   4. parse item IDs, brand / rights owner, reason, reference ID
//   5. flag matching products do_not_relist  +  add the brand to vero_brands
//
// SAFETY
//   • Never ends or deletes a listing — flagging only. Ending stays manual.
//   • Every notice is stored in vero_notices so you can audit what it did.
//   • Brand auto-add only fires on a confident brand extraction; otherwise the
//     notice is stored as needs_review and nothing is guessed.

const vero = require('./vero');

let _pool = null;

// Subjects/bodies that indicate an IP or prohibited-items action
const NOTICE_PATTERNS = [
  /vero/i,
  /intellectual property/i,
  /verified rights owner/i,
  /infring/i,
  /counterfeit/i,
  /unauthorized (listing|item|copies)/i,
  /prohibited items?/i,
  /we removed your listing/i,
  /listing (was |has been )?removed/i,
  /didn'?t follow our .{0,40}polic/i,
  /selling (privileges|restrictions?)/i,
  /trademark|copyright/i,
];

function looksLikeNotice(text) {
  return NOTICE_PATTERNS.some(re => re.test(text || ''));
}

async function initInbox(pool) {
  _pool = pool;
  await _pool.query(`
    CREATE TABLE IF NOT EXISTS vero_notices (
      message_id   TEXT PRIMARY KEY,
      account_id   TEXT NOT NULL DEFAULT 'default',
      received_at  TIMESTAMPTZ,
      subject      TEXT,
      body         TEXT,
      item_ids     TEXT[],
      brands       TEXT[],
      reference_id TEXT,
      matched_ids  TEXT[],
      flagged      INTEGER NOT NULL DEFAULT 0,
      needs_review BOOLEAN NOT NULL DEFAULT FALSE,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS vero_notices_acct_idx ON vero_notices(account_id, received_at DESC);
  `).catch(e => console.warn('[vero-inbox] schema:', e.message));
  console.log('[vero-inbox] ready');
}

// ── Trading API helpers ──────────────────────────────────────────────────────
function xmlEsc(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

async function tradingCall(tradingUrl, callName, token, innerXml) {
  const body = `<?xml version="1.0" encoding="utf-8"?>
<${callName}Request xmlns="urn:ebay:apis:eBLBaseComponents">${innerXml}</${callName}Request>`;
  const r = await fetch(tradingUrl, {
    method: 'POST',
    headers: {
      'X-EBAY-API-COMPATIBILITY-LEVEL': '1193',
      'X-EBAY-API-CALL-NAME': callName,
      'X-EBAY-API-SITEID': '0',
      'X-EBAY-API-IAF-TOKEN': token,
      'Content-Type': 'text/xml',
    },
    body,
  });
  return r.text();
}

// Minimal, dependency-free extraction of repeated <Message> blocks
function parseMessages(xml) {
  const out = [];
  const blocks = xml.match(/<Message>[\s\S]*?<\/Message>/g) || [];
  for (const b of blocks) {
    const g = (tag) => {
      const m = b.match(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`));
      return m ? m[1].trim() : '';
    };
    out.push({
      messageId:  g('MessageID'),
      subject:    g('Subject'),
      text:       g('Text') || g('Content'),
      flagged:    g('Flagged') === 'true',
      receiveDate: g('ReceiveDate'),
      sender:     g('Sender'),
      itemId:     g('ItemID'),
    });
  }
  return out;
}

function stripHtml(s) {
  return String(s || '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&#39;|&rsquo;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/[ \t]+/g, ' ')
    .trim();
}

// Words that mean we captured sentence noise, not a brand name
const _BRAND_NOISE = /(policy|listing|item|rights? owner|report|ebay|account|program|violation|infring|notice|seller)/i;

function _cleanBrand(raw) {
  let b = String(raw || '')
    .replace(/^[\s"'(]+|[\s"'.),;:]+$/g, '')
    .replace(/^(the|a|an)\s+/i, '')
    // drop trailing verb phrases: "adidas AG reported this listing"
    .replace(/\s+(has|have|had)?\s*(reported|filed|submitted|requested|claimed|notified)\b.*$/i, '')
    .replace(/\s+(inc|llc|ltd|gmbh|ag|sa|s\.a\.|co|corp|corporation|company|group)\.?$/i, '')
    .trim();
  if (!b || b.length < 2 || b.length > 40) return null;
  if (b.split(/\s+/).length > 4) return null;      // sentence fragment, not a brand
  if (_BRAND_NOISE.test(b)) return null;
  if (!/[a-z]/i.test(b)) return null;
  return b.toLowerCase();
}

// ── Parsing a notice ─────────────────────────────────────────────────────────
function parseNotice(subject, bodyText) {
  const text = `${subject}\n${bodyText}`;
  // Reference IDs ("2-107679766053") contain a 12-digit run that would
  // otherwise be mistaken for an eBay item ID and flag the wrong listing.
  const refM = text.match(/Reference\s*ID:?\s*([\w-]+)/i);
  const scrubbed = text.replace(/Reference\s*ID:?\s*[\w-]+/ig, ' ');
  // eBay item IDs are 10-12 digits. Prefer ones introduced by listing/item.
  const anchored = [...scrubbed.matchAll(/(?:listing|item)(?:\s*(?:id|#|number))?\s*[:#]?\s*(\d{10,12})\b/ig)]
    .map(m => m[1]);
  const loose = (scrubbed.match(/\b\d{10,12}\b/g) || []);
  const itemIds = [...new Set(anchored.length ? anchored : loose)]
    .filter(id => !/^20\d{6}/.test(id));

  const brands = [];
  const ownerPatterns = [
    /rights?\s+owner(?:\s+is)?[:\s]+([^.,;\n]{2,45})/i,
    /reported\s+by[:\s]+([^.,;\n]{2,45})/i,
    /on\s+behalf\s+of[:\s]+([^.,;\n]{2,45})/i,
    /notice\s+from[:\s]+([^.,;\n]{2,45})/i,
  ];
  for (const re of ownerPatterns) {
    const m = bodyText.match(re);
    const c = m && _cleanBrand(m[1]);
    if (c) { brands.push(c); break; }
  }
  // Known-brand mentions anywhere in the notice
  const known = [...vero.TIER_CRITICAL, ...vero.TIER_HIGH, ...vero.TIER_MEDIUM];
  const low = text.toLowerCase();
  for (const b of known) {
    if (new RegExp(`(^|[^a-z0-9])${b.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}([^a-z0-9]|$)`, 'i').test(low)) {
      brands.push(b);
    }
  }
  return {
    itemIds,
    brands: [...new Set(brands)],
    referenceId: refM ? refM[1] : null,
  };
}

// ── Main scan ────────────────────────────────────────────────────────────────
/**
 * Scan My Messages for policy/VeRO notices and act on them.
 * @param {object} o { token, accountId, tradingUrl, days, autoFlag, autoAddBrand }
 */
async function scanMessages({ token, accountId = 'default', tradingUrl,
                              days = 30, autoFlag = true, autoAddBrand = true }) {
  if (!token) throw new Error('access_token required');
  const end = new Date();
  const start = new Date(Date.now() - Math.min(90, Math.max(1, days)) * 86400000);

  // 1) headers
  const headXml = await tradingCall(tradingUrl, 'GetMyMessages', token,
    `<DetailLevel>ReturnHeaders</DetailLevel>` +
    `<StartTime>${start.toISOString()}</StartTime>` +
    `<EndTime>${end.toISOString()}</EndTime>`);
  if (/<Ack>Failure<\/Ack>/.test(headXml)) {
    const err = (headXml.match(/<LongMessage>([\s\S]*?)<\/LongMessage>/) || [])[1] || 'unknown';
    throw new Error('GetMyMessages failed: ' + stripHtml(err).slice(0, 200));
  }
  const heads = parseMessages(headXml);
  const candidates = heads.filter(m => m.messageId && (m.flagged || looksLikeNotice(m.subject)));
  if (!candidates.length) {
    return { scanned: heads.length, notices: [], flagged: 0, brandsAdded: [] };
  }

  // 2) bodies — max 10 message IDs per call
  const detailed = [];
  for (let i = 0; i < candidates.length; i += 10) {
    const ids = candidates.slice(i, i + 10);
    const idXml = ids.map(m => `<MessageID>${xmlEsc(m.messageId)}</MessageID>`).join('');
    const bodyXml = await tradingCall(tradingUrl, 'GetMyMessages', token,
      `<DetailLevel>ReturnMessages</DetailLevel><MessageIDs>${idXml}</MessageIDs>`);
    detailed.push(...parseMessages(bodyXml));
    await new Promise(r => setTimeout(r, 250));
  }

  // 3) parse + act
  const results = [];
  const brandsAdded = [];
  let totalFlagged = 0;

  for (const m of detailed) {
    const subject = stripHtml(m.subject);
    const body = stripHtml(m.text);
    if (!looksLikeNotice(`${subject} ${body}`)) continue;

    const parsed = parseNotice(subject, body);
    if (m.itemId && !parsed.itemIds.includes(m.itemId)) parsed.itemIds.unshift(m.itemId);

    // Match item IDs to products in THIS account
    let matched = [];
    if (parsed.itemIds.length && _pool) {
      const r = await _pool.query(
        `SELECT id, title, ebay_item_id, ebay_sku FROM products
          WHERE account_id = $1 AND ebay_item_id = ANY($2)`,
        [accountId, parsed.itemIds]).catch(() => ({ rows: [] }));
      matched = r.rows;
    }

    // Flag matched listings — never end them, only stop future syncing/listing
    let flaggedNow = 0;
    if (autoFlag && matched.length && _pool) {
      const ids = matched.map(x => x.id);
      const up = await _pool.query(
        `UPDATE products SET do_not_relist = TRUE,
                vero_note = $2, updated_at = NOW()
          WHERE id = ANY($1)`,
        [ids, `eBay notice: ${subject.slice(0, 160)}`]).catch(() => ({ rowCount: 0 }));
      flaggedNow = up.rowCount || 0;
      totalFlagged += flaggedNow;
      await _pool.query(
        `DELETE FROM relay_state WHERE ebay_sku IN (
           SELECT ebay_sku FROM products WHERE id = ANY($1) AND ebay_sku IS NOT NULL)`,
        [ids]).catch(() => {});
    }

    // Add the offending brand to the permanent blocklist so nothing similar
    // gets imported again. Only when we actually identified a brand.
    if (autoAddBrand && parsed.brands.length && _pool) {
      for (const b of parsed.brands.slice(0, 3)) {
        if (b.length < 2 || b.length > 40) continue;
        await _pool.query(
          `INSERT INTO vero_brands(brand, tier) VALUES($1,'critical')
           ON CONFLICT(brand) DO NOTHING`, [b]).catch(() => {});
        brandsAdded.push(b);
      }
    }

    const needsReview = parsed.itemIds.length === 0 || matched.length === 0;
    if (_pool) {
      await _pool.query(
        `INSERT INTO vero_notices(message_id, account_id, received_at, subject, body,
                                  item_ids, brands, reference_id, matched_ids, flagged, needs_review)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
         ON CONFLICT(message_id) DO UPDATE
           SET matched_ids = EXCLUDED.matched_ids, flagged = EXCLUDED.flagged,
               needs_review = EXCLUDED.needs_review`,
        [m.messageId, accountId, m.receiveDate ? new Date(m.receiveDate) : new Date(),
         subject.slice(0, 500), body.slice(0, 8000),
         parsed.itemIds, parsed.brands, parsed.referenceId,
         matched.map(x => x.id), flaggedNow, needsReview]).catch(e =>
        console.warn('[vero-inbox] store notice:', e.message));
    }

    results.push({
      messageId: m.messageId, received: m.receiveDate, subject,
      itemIds: parsed.itemIds, brands: parsed.brands,
      referenceId: parsed.referenceId,
      matchedTitles: matched.map(x => x.title),
      flagged: flaggedNow, needsReview,
    });
  }

  console.log(`[vero-inbox] ${accountId}: ${results.length} notices, ${totalFlagged} listings flagged, brands+${[...new Set(brandsAdded)].join(',') || 'none'}`);
  return {
    scanned: heads.length,
    notices: results,
    flagged: totalFlagged,
    brandsAdded: [...new Set(brandsAdded)],
  };
}

// ── Routes ───────────────────────────────────────────────────────────────────
function mountInbox(app, getEbayUrls) {
  const acct = req => {
    const a = String(req.query.account || '').trim();
    return /^[\w.\-]{1,64}$/.test(a) ? a : 'default';
  };

  app.post('/api/vero/scan-messages', async (req, res) => {
    try {
      const { access_token, days = 30, autoFlag = true, autoAddBrand = true } = req.body || {};
      if (!access_token) return res.status(400).json({ error: 'access_token required' });
      // Resolve identity from the token itself so notices land on the right account
      const ebay = require('./ebay');
      let accountId = acct(req);
      try {
        const real = await ebay.resolveAccountId(access_token);
        if (real && real !== 'default') accountId = real;
      } catch (e) {}
      const out = await scanMessages({
        token: access_token, accountId,
        tradingUrl: getEbayUrls().EBAY_TRADING,
        days, autoFlag, autoAddBrand,
      });
      res.json({ success: true, accountId, ...out });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/api/vero/notices', async (req, res) => {
    try {
      const r = await _pool.query(
        `SELECT message_id, received_at, subject, item_ids, brands, reference_id,
                matched_ids, flagged, needs_review
           FROM vero_notices WHERE account_id = $1
          ORDER BY received_at DESC LIMIT 200`, [acct(req)]);
      res.json({ notices: r.rows });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  console.log('[vero-inbox] routes mounted: /api/vero/scan-messages, /api/vero/notices');
}

module.exports = { initInbox, mountInbox, scanMessages, parseNotice, looksLikeNotice };
