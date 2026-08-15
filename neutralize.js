// neutralize.js — make a multi-variant listing description variant-neutral
//
// THE PROBLEM
// Amazon's parent page description describes ONE variant — usually whichever
// the scraper landed on. Push that to a multi-variant eBay listing and every
// other variant is misdescribed: the description says "100 Pack" while the
// buyer selected the 1000 pack, or says "Black" on a listing selling six
// colours. That is a misleading listing: it drives INADs, "not as described"
// returns, and negative feedback — and eBay holds the SELLER responsible for
// listing accuracy regardless of where the copy came from.
//
// THE APPROACH
// A claim is variant-specific when it names a value that DIFFERS across the
// variants in this listing. "100 Pack" is only misleading because a 1000 pack
// exists in the same listing; on a single-variant listing it's simply accurate.
// So neutralization is driven by the listing's own variation values:
//
//   1. Collect every distinct value per dimension (colour, size, pack, style…).
//   2. A dimension with 2+ distinct values is CONFLICTING.
//   3. Remove sentences/bullets asserting one specific conflicting value.
//   4. Also strip standalone quantity/measurement claims when the listing
//      varies by quantity/size, even if they don't exactly match a value
//      ("holds 100 sheets" on a pack-size listing).
//   5. Append a neutral pointer to the variation selector.
//
// Conservative on purpose: it only drops a sentence when the value it asserts
// actually conflicts, so genuinely shared product info survives intact.

// Units that indicate a quantity/measurement claim
const UNIT = String.raw`(?:pack|packs|pcs|pieces?|count|ct|pairs?|sets?|sheets?|rolls?|bags?|boxes?|bottles?|tablets?|capsules?|servings?|oz|ounces?|ml|l|liters?|litres?|g|grams?|kg|lbs?|pounds?|in|inch|inches|ft|feet|cm|mm|m|yards?|gal|gallons?|w|watts?|v|volts?|mah|gb|tb)`;

const QTY_PATTERNS = [
  new RegExp(String.raw`\b(?:pack|set|box|case|lot|bundle)\s+of\s+\d[\d,.]*\b`, 'i'),
  new RegExp(String.raw`\b\d[\d,.]*\s*[-–]?\s*${UNIT}\b`, 'i'),
  new RegExp(String.raw`\b\d[\d,.]*\s*x\s*\d[\d,.]*\s*(?:${UNIT})?\b`, 'i'),
  new RegExp(String.raw`\bsize[:\s]+\S+`, 'i'),
  new RegExp(String.raw`\bcolou?r[:\s]+\S+`, 'i'),
];

// Amazon / marketplace artefacts that shouldn't appear on an eBay listing at
// all (also reduces the copied-content exposure that draws VeRO reports).
const ARTEFACTS = [
  /visit the [^.<|]{2,60}\bstore\b[^.<]*\.?/gi,
  /\babout this item\b/gi,
  /\bamazon(?:'s|\.com)?\b/gi,
  /\bprime\s+(?:delivery|shipping|eligible)\b/gi,
  /\bsee more product details\b[^.<]*\.?/gi,
  /\bclick (?:here|the button)[^.<]*/gi,
  /\badd to cart\b/gi,
  /\bbuy (?:now|it now)\b/gi,
  /\bas seen on tv\b/gi,
  /\bfree (?:returns?|shipping)\b/gi,      // shipping terms are eBay's, not the copy's
  /\b(?:one[- ]day|two[- ]day|same[- ]day) (?:delivery|shipping)\b/gi,
  /\bASIN[:\s]*[A-Z0-9]{10}\b/gi,
  /\b\d+(?:\.\d+)?\s*out of\s*5\s*stars?\b/gi,
  /\b\d[\d,]*\s*(?:customer\s*)?(?:ratings?|reviews?)\b/gi,
  /\bbest ?seller(?:s)?(?:\s*rank)?\b/gi,
  /\b#\d[\d,]*\s*in\s+[A-Z][^.<]{2,40}/g,
];

function _norm(s) {
  return String(s || '').toLowerCase().replace(/\s+/g, ' ').trim();
}

function _escape(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Work out which variation values conflict within this listing.
 * @param {object} variations  { dimName: [values] }  OR  comboAsin keys
 * @returns {{ conflicting: string[], varyByQty: boolean, dims: object }}
 */
function conflictingValues(variations) {
  const dims = {};
  if (Array.isArray(variations)) {
    // list of combo keys like "Black/Wine Red|Large"
    for (const key of variations) {
      const parts = String(key).split('|').map(s => s.trim()).filter(Boolean);
      parts.forEach((p, i) => {
        (dims[`d${i}`] = dims[`d${i}`] || new Set()).add(p);
      });
    }
  } else if (variations && typeof variations === 'object') {
    for (const [k, vals] of Object.entries(variations)) {
      const arr = Array.isArray(vals) ? vals : [vals];
      dims[k] = new Set(arr.map(v => String(v).trim()).filter(Boolean));
    }
  }
  const conflicting = [];
  const conflictUnits = new Set();
  let varyByQty = false;
  for (const set of Object.values(dims)) {
    if (set.size < 2) continue;             // single value → not misleading
    for (const v of set) {
      conflicting.push(v);
      if (QTY_PATTERNS.some(re => re.test(v))) {
        varyByQty = true;
        // Record WHICH units vary. A listing varying by "100 Pack / 1000 Pack"
        // varies by pack count — it does NOT vary by fluid ounces, so
        // "each cup holds 12 oz" is shared spec and must be kept. Matching on
        // every unit blindly strips accurate, useful copy.
        const um = String(v).match(new RegExp(UNIT, 'ig'));
        for (const u of (um || [])) conflictUnits.add(u.toLowerCase());
        if (/\bpack|set|box|case|lot|bundle\b/i.test(v)) conflictUnits.add('__count__');
      }
    }
  }
  return { conflicting, varyByQty, conflictUnits, dims };
}

/** Split HTML/text into addressable chunks (list items, paragraphs, sentences). */
function _chunks(html) {
  const out = [];
  const re = /<li[^>]*>([\s\S]*?)<\/li>|<p[^>]*>([\s\S]*?)<\/p>/gi;
  let m, last = 0, found = false;
  while ((m = re.exec(html)) !== null) {
    found = true;
    if (m.index > last) out.push({ text: html.slice(last, m.index), tag: null });
    out.push({ text: m[0], inner: m[1] ?? m[2] ?? '',
               tag: (m[0].match(/^<\s*(li|p)\b/i) || [])[1]?.toLowerCase() || null });
    last = m.index + m[0].length;
  }
  if (last < html.length) out.push({ text: html.slice(last), tag: null });
  return found ? out : [{ text: html, tag: null }];
}

/**
 * Neutralize a description for a multi-variant listing.
 * @returns {{ description: string, removed: string[], changed: boolean }}
 */
function neutralizeDescription(desc, opts = {}) {
  if (!desc) return { description: desc, removed: [], changed: false };
  const { variations, variantCount = 0, addNote = true } = opts;
  const { conflicting, varyByQty, conflictUnits } = conflictingValues(variations);
  // Only quantity claims in the SAME units the listing varies by are risky.
  const _units = [...(conflictUnits || [])].filter(u => u !== '__count__');
  const _qtyRes = [];
  if (varyByQty) {
    if (conflictUnits && conflictUnits.has('__count__')) {
      _qtyRes.push(new RegExp(String.raw`\b(?:pack|set|box|case|lot|bundle)\s+of\s+\d[\d,.]*\b`, 'i'));
      _qtyRes.push(new RegExp(String.raw`\b\d[\d,.]*\s*[-–]?\s*(?:pack|packs|pcs|pieces?|count|ct|sets?)\b`, 'i'));
    }
    for (const u of _units) {
      _qtyRes.push(new RegExp(String.raw`\b\d[\d,.]*\s*[-–]?\s*${_escape(u)}\b`, 'i'));
    }
  }
  const _hitsQty = (txt) => _qtyRes.some(re => re.test(txt));
  const removed = [];
  let d = String(desc);

  // 1) Marketplace artefacts — always removed, single-variant or not
  for (const re of ARTEFACTS) d = d.replace(re, ' ');

  // Nothing varies → no variant-specific risk, just return the cleaned copy
  if (!conflicting.length && variantCount < 2) {
    d = d.replace(/\s{2,}/g, ' ').replace(/<(li|p)[^>]*>\s*<\/\1>/gi, '').trim();
    return { description: d, removed, changed: d !== String(desc) };
  }

  // 2) Match values that actually conflict across variants
  const valueRes = conflicting
    .filter(v => v.length >= 2 && v.length <= 40)
    .map(v => new RegExp(`(^|[^a-z0-9])${_escape(v)}([^a-z0-9]|$)`, 'i'));

  const chunks = _chunks(d);
  const kept = [];
  for (const c of chunks) {
    const inner = c.inner != null ? c.inner : c.text;
    const plain = inner.replace(/<[^>]+>/g, ' ');
    if (!plain.trim()) { kept.push(c.text); continue; }

    // Asserts a value that differs between variants → misleading for the rest
    const hitsValue = valueRes.some(re => re.test(plain));
    // Listing varies by quantity/size and this chunk makes a quantity claim
    const hitsQty = _hitsQty(plain);

    if (hitsValue || hitsQty) {
      // A bullet is one discrete claim → drop it whole.
      // A paragraph holds several claims → drop only the offending sentences,
      // so shared product info in the same paragraph survives.
      if (c.tag === 'li') {
        removed.push(plain.replace(/\s+/g, ' ').trim().slice(0, 120));
        continue;
      }
      const sentences = inner.split(/(?<=[.!?])\s+/);
      const survivors = sentences.filter(s2 => {
        const sp = s2.replace(/<[^>]+>/g, ' ');
        const bad = valueRes.some(re => re.test(sp)) || _hitsQty(sp);
        if (bad) removed.push(sp.replace(/\s+/g, ' ').trim().slice(0, 120));
        return !bad;
      });
      const body = survivors.join(' ').trim();
      if (c.tag === 'p') { if (body) kept.push('<p>' + body + '</p>'); }
      else kept.push(body);
      continue;
    }
    kept.push(c.text);
  }
  d = kept.join('');

  // 3) Tidy up empties left behind
  d = d.replace(/<(li|p)[^>]*>\s*<\/\1>/gi, '')
       .replace(/<ul>\s*<\/ul>/gi, '')
       .replace(/\s{2,}/g, ' ')
       .replace(/\s+([.,;:])/g, '$1')
       .trim();

  // 4) Point the buyer at the variation selector instead of a specific value
  if (addNote && (removed.length || variantCount >= 2)) {
    const note = '<p><strong>Please choose your option from the menu above.</strong> '
      + 'This listing covers several options; the item you receive is the one you select at checkout. '
      + 'Specifications such as size, colour, and quantity vary by option.</p>';
    if (!/choose your option from the menu/i.test(d)) d += note;
  }

  return { description: d, removed, changed: true };
}

module.exports = { neutralizeDescription, conflictingValues, QTY_PATTERNS };
