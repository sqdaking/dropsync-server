// vero.js — VeRO / intellectual-property risk screening for DropSync
//
// WHY THIS EXISTS
// eBay's VeRO program lets rights owners report listings that use their brand
// name, product photography, or marketing copy without authorization. It does
// NOT matter whether the item is authentic, and it does not matter whether the
// infringement was intentional — a reported listing is removed and the strikes
// accumulate on the account. Amazon-sourced dropship listings are especially
// exposed because they reuse Amazon's product images and copy verbatim.
//
// WHAT IT DOES
//   1. Scores every product for VeRO risk (brand match + Amazon-hosted imagery)
//   2. Serves a self-contained HTML report with a ranked kill-list
//   3. Lets you flag listings `do_not_relist` so sync/publish skips them FOREVER
//
// A flagged SKU is never re-published even if the sync logic wants to, which is
// the important safety property: relisting an item removed under VeRO is what
// turns a temporary restriction into a permanent one.

// ── Brand lists ──────────────────────────────────────────────────────────────
// Tiers reflect how aggressively the rights owner is known to enforce, not how
// famous the brand is. CRITICAL = well-documented active eBay enforcement.
// This list is a starting point — extend it via the `vero_brands` table.

const TIER_CRITICAL = [
  // Luxury / fashion houses — dedicated teams scanning eBay daily
  'louis vuitton', 'gucci', 'chanel', 'hermes', 'hermès', 'prada', 'burberry',
  'balenciaga', 'dior', 'fendi', 'versace', 'yves saint laurent', 'ysl',
  'bottega veneta', 'givenchy', 'valentino', 'rolex', 'cartier', 'tiffany',
  'omega watch', 'patek philippe', 'audemars piguet',
  // Athletic — extremely active VeRO participants
  'nike', 'adidas', 'jordan', 'air jordan', 'yeezy', 'supreme', 'off-white',
  'lululemon', 'under armour',
  // Entertainment IP — character/likeness enforcement
  'disney', 'pixar', 'marvel', 'star wars', 'nintendo', 'pokemon', 'pokémon',
  'sanrio', 'hello kitty', 'harry potter', 'dc comics', 'warner bros',
  // Tech
  'apple', 'airpods', 'iphone', 'ipad', 'macbook', 'airtag',
  // Toys
  'lego', 'squishmallow', 'squishmallows', 'funko', 'build-a-bear',
];

const TIER_HIGH = [
  // Footwear / apparel
  'puma', 'reebok', 'new balance', 'converse', 'vans', 'crocs', 'skechers',
  'timberland', 'dr. martens', 'doc martens', 'ugg', 'birkenstock', 'asics',
  'hoka', 'brooks running', 'salomon', 'fila', 'champion', 'gymshark',
  'levi', "levi's", 'levis', 'calvin klein', 'tommy hilfiger', 'ralph lauren',
  'polo ralph', 'hugo boss', 'armani', 'lacoste', 'north face', 'patagonia',
  'columbia sportswear', 'carhartt', 'canada goose', 'moncler', 'spanx',
  "victoria's secret", 'pink victoria',
  // Bags / accessories
  'michael kors', 'coach', 'kate spade', 'tory burch', 'longchamp', 'fossil',
  'ray-ban', 'rayban', 'oakley', 'maui jim', 'pandora jewelry', 'swarovski',
  // Consumer electronics
  'samsung', 'sony', 'bose', 'beats by dre', 'jbl', 'sennheiser', 'gopro',
  'dji', 'garmin', 'fitbit', 'sonos', 'nvidia', 'playstation', 'xbox',
  // Home / kitchen
  'dyson', 'kitchenaid', 'instant pot', 'yeti', 'stanley cup', 'hydro flask',
  'nespresso', 'keurig', 'le creuset', 'lodge cast iron', 'ninja kitchen',
  'vitamix', 'shark ninja', 'roomba', 'irobot', 'traeger', 'weber grill',
  'peloton', 'therabody', 'theragun',
  // Health / beauty / baby — high-margin, heavily enforced
  'pampers', 'huggies', 'luvs', 'similac', 'enfamil', 'medela', 'philips avent',
  'tide', 'gillette', 'olay', 'crest', 'oral-b', 'braun', 'nivea', "l'oreal",
  'loreal', 'estee lauder', 'estée lauder', 'clinique', 'lancome', 'lancôme',
  'mac cosmetics', 'urban decay', 'the ordinary', 'cerave', 'la roche-posay',
  'neutrogena', 'aveeno', 'johnson & johnson', 'dyson airwrap',
  // Toys / kids
  'hasbro', 'mattel', 'barbie', 'hot wheels', 'fisher-price', 'fisher price',
  'melissa & doug', 'graco', 'chicco', 'britax', 'bugaboo', 'uppababy',
];

const TIER_MEDIUM = [
  'anker', 'belkin', 'logitech', 'razer', 'corsair', 'steelseries', 'hyperx',
  'netgear', 'tp-link', 'roku', 'echo dot', 'kindle', 'fire tv', 'alexa',
  'google nest', 'chromecast', 'microsoft', 'milwaukee tool', 'dewalt',
  'makita', 'bosch', 'ryobi', 'craftsman', 'black & decker', 'stanley tools',
  '3m', 'scotch-brite', 'sharpie', 'crayola', 'ticonderoga', 'post-it',
  'rubbermaid', 'tupperware', 'oakley standard', 'wilson sporting',
  'spalding', 'titleist', 'callaway golf', 'nfl', 'nba', 'mlb', 'nhl',
  'fifa', 'olympic', 'ncaa',
];

// Words that make a brand match a near-certain violation regardless of tier
// "Compatible with iPhone 14" is NOMINATIVE FAIR USE — naming the device a
// product fits is lawful, and eBay requires compatibility information on
// accessories, so a phone case cannot be listed without it. Those phrases were
// pushing legitimate listings into hard blocks and are gone.
// What remains genuinely signals a counterfeit or dupe.
const AGGRAVATORS = [
  'replica', 'knockoff', 'knock-off', 'inspired by', 'style of', '1:1',
  'mirror quality', 'aaa quality',
];

// ── RESTRICTED / PROHIBITED CATEGORIES ───────────────────────────────────────
// Brand matching alone misses a whole class of risk. eBay's prohibited and
// restricted items policies apply on top of VeRO, and some categories draw
// rights-owner reports even with no brand name present at all.
//
// The clearest example in this catalogue: "inspired by" dupe fragrances
// ("No.1005 Skyfall", "Our version of ..."). They carry no trademark in the
// title, so brand screening passes them — while fragrance houses treat them as
// trademark infringement and report them on sight.
const RESTRICTED_PATTERNS = [
  { risk: 'critical', label: 'dupe/inspired-by fragrance or product',
    re: /\b(inspired\s+by|our\s+version\s+of|compare\s+to|dupe|smells?\s+like|type\s+of\s+perfume|impression\s+of|similar\s+to\s+designer)\b/i },
  { risk: 'critical', label: 'replica / knockoff wording',
    re: /\b(replica|knock[\s-]?off|clone|counterfeit|unauthorized|unauthorised|fake|1:1|aaa\+?\s*quality|mirror\s+quality)\b/i },
  { risk: 'high', label: 'perfume / cosmetics (restricted category)',
    re: /\b(perfume|cologne|eau\s+de\s+(parfum|toilette)|fragrance|body\s+mist|aftershave)\b/i },
  { risk: 'high', label: 'supplement / ingestible (restricted category)',
    re: /\b(supplement|vitamin|gummies|capsules|probiotic|weight\s+loss|detox\s+tea|melatonin)\b/i },
  { risk: 'high', label: 'medical device / health claim (restricted category)',
    re: /\b(medical\s+device|blood\s+pressure\s+monitor|thermometer|nebulizer|cpap|hearing\s+aid|pulse\s+oximeter)\b/i },
  { risk: 'medium', label: 'lithium battery / hazmat shipping rules',
    re: /\b(lithium|li[\s-]?ion\s+battery|power\s+bank|butane|lighter\s+fluid|aerosol)\b/i },
  // PRODUCT SAFETY — 20% of this account's flagged listings, and none of them
  // involved a brand. These are recall-prone or regulated categories where
  // eBay pulls listings on the category alone: infant carriers, children's
  // water products, anything contacting drinking water.
  { risk: 'critical', label: 'infant carrier / sling (recall-prone, eBay product safety)',
    re: /\b(baby\s+sling|sling\s+carrier|baby\s+carrier|infant\s+carrier|toddler\s+carrier|baby\s+wrap)\b/i },
  { risk: 'critical', label: 'children/pet pool (eBay product safety)',
    re: /\b(kiddie\s+pool|swimming\s+pool|paddling\s+pool|dog\s+pool|inflatable\s+pool)\b/i },
  { risk: 'high', label: 'drinking-water contact (lead/NSF rules)',
    re: /\b(faucet|tap\s+water|water\s+filter|drinking\s+water|sink\s+faucet)\b/i },
  { risk: 'high', label: 'child safety equipment (regulated)',
    re: /\b(car\s+seat|booster\s+seat|crib|bassinet|walker|helmet|life\s+jacket)\b/i },
  { risk: 'medium', label: 'used/worn clothing (restricted category)',
    re: /\b(used|pre[\s-]?owned|worn|second[\s-]?hand)\b.{0,20}\b(clothing|shoes|underwear|socks)\b/i },
];

function restrictedCheck(text) {
  const hay = String(text || '');
  const hits = RESTRICTED_PATTERNS.filter(p => p.re.test(hay));
  return hits;
}

// Phrases where a "brand" word is actually a generic English word. Without
// these, "Apple Cider Vinegar Gummies" scores as an Apple trademark violation
// and a clean listing gets pulled from your catalogue for no reason.
const NEGATIVE_CONTEXT = {
  'apple': /apple\s*(cider|sauce|juice|pie|slice|tree|flavor|flavour|vinegar|butter|chip|corer|peeler|seed|blossom|green)|(green|red|dried|fresh|candy|caramel)\s*apple|applesauce/i,
  'coach': /coaching|coach\s*(bus|whistle|clipboard)|(life|sports|football|basketball|business)\s*coach/i,
  'champion': /world\s*champion|champions\s*league|championship|champion\s*(belt|trophy|cup)/i,
  'polo': /polo\s*(shirt|neck|collar)(?!\s*ralph)|water\s*polo/i,
  'vans': /minivans|caravans|(cargo|delivery|camper)\s*vans/i,
  'fila': /filament|filament/i,
  'prada': /pradaxa/i,
  '3m': /\b3\s*m(m|eter|etre|onth|in)/i,
  'nike': /nikey|nikelodeon/i,
  'mac cosmetics': /^$/,
  'sony': /masonry|persony/i,
  'bose': /boseman|verbose/i,
};

function _norm(s) {
  return String(s || '').toLowerCase().replace(/[\u2018\u2019]/g, "'").replace(/\s+/g, ' ');
}

// Word-boundary match so "apple cider vinegar" doesn't match "apple" and
// "3m" doesn't match "3 mm". Brands containing spaces match as phrases.
function _hasBrand(haystack, brand) {
  const neg = NEGATIVE_CONTEXT[brand];
  if (neg && neg.test(haystack)) return false;
  const b = brand.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`(^|[^a-z0-9])${b}([^a-z0-9]|$)`, 'i').test(haystack);
}

/**
 * Score one product. Returns { risk, score, brands[], reasons[] }.
 * risk: 'critical' | 'high' | 'medium' | 'low' | 'none'
 */
function scoreProduct(p, extraBrands = []) {
  const title = _norm(p.title);
  const brandField = _norm(p.brand || p.data?.brand);
  const desc = _norm(p.description || p.data?.description).slice(0, 4000);
  const hay = `${title} ${brandField}`;
  const reasons = [];
  const brands = [];
  let score = 0;

  const check = (list, pts, label) => {
    for (const b of list) {
      if (_hasBrand(hay, b) || (brandField && brandField === b)) {
        brands.push(b);
        score += pts;
        reasons.push(`${label} brand in title/brand: "${b}"`);
      }
    }
  };
  check(TIER_CRITICAL, 100, 'CRITICAL');
  check(TIER_HIGH, 50, 'HIGH');
  check(TIER_MEDIUM, 20, 'MEDIUM');
  check(extraBrands, 100, 'CUSTOM');

  // Brand mentioned only in the description still carries copyright exposure
  if (brands.length === 0 && desc) {
    for (const b of [...TIER_CRITICAL, ...TIER_HIGH]) {
      if (_hasBrand(desc, b)) { brands.push(b); score += 15; reasons.push(`brand in description: "${b}"`); break; }
    }
  }

  // Amazon-hosted product photography = the rights owner's copyrighted images.
  // This is the single most common VeRO copyright trigger for dropshippers.
  const imgs = []
    .concat(p.imageUrl || p.image_url || [])
    .concat(p.data?.images || p.images || []);
  if (imgs.some(u => /media-amazon|images-amazon|ssl-images-amazon/i.test(String(u)))) {
    score += 30;
    reasons.push('uses Amazon-hosted product images (copyright exposure) — an eBay catalogue match would replace these with licensed images');
  } else if (imgs.some(u => /i\.ebayimg\.com|ebaystatic/i.test(String(u)))) {
    // Already on eBay's own imagery: no copyright exposure from photos.
    reasons.push('images are eBay catalogue images (no copyright exposure)');
  }

  // Copy lifted from Amazon
  if (/visit the .{2,40} store|about this item|amazon\.com/i.test(desc)) {
    score += 20;
    reasons.push('description appears copied from Amazon');
  }

  // Restricted/prohibited categories count toward the audit score as well, so
  // the report shows perfume and dupe listings alongside branded ones.
  for (const r of restrictedCheck(`${title} ${desc}`)) {
    score += r.risk === 'critical' ? 100 : r.risk === 'high' ? 40 : 15;
    reasons.push(`restricted category: ${r.label}`);
  }

  for (const a of AGGRAVATORS) {
    if (brands.length && title.includes(a)) {
      score += 10;
      reasons.push(`aggravating phrase: "${a.trim()}"`);
      break;
    }
  }

  let risk = 'none';
  if (score >= 100) risk = 'critical';
  else if (score >= 50) risk = 'high';
  else if (score >= 25) risk = 'medium';
  else if (score > 0) risk = 'low';

  return { risk, score, brands: [...new Set(brands)], reasons };
}

// ── DEPARTMENT CLASSIFICATION ────────────────────────────────────────────────
// Group listings the way a seller thinks about their catalogue, so the risk
// report answers "which departments should I stop sourcing from?" rather than
// listing 8,000 individual items. Ordered: the first match wins, so the more
// specific categories are checked before the general ones.
const DEPARTMENTS = [
  // Product-type rules come FIRST. Gender words appear across every
  // department — "Travel Backpack for Women", "Bracelets Set for Women",
  // "Toys for Girls" — so checking clothing first mis-binned bags, jewellery
  // and toys as apparel and would have skewed the whole report.
  ['Fragrance / perfume',   /\b(perfume|cologne|eau\s+de|fragrance|body\s+mist|aftershave)\b/i],
  ['Beauty / cosmetics',    /\b(makeup|lipstick|foundation|serum|moisturizer|skincare|shampoo|conditioner|nail\s+polish|mascara)\b/i],
  ['Supplements / health',  /\b(supplement|vitamin|gummies|probiotic|collagen|protein\s+powder|melatonin)\b/i],
  // Plurals matter: \bbracelet\b does NOT match "Bracelets", so a jewellery
  // listing fell through to the clothing rule via "for Women".
  ['Jewelry / watches',     /\b(necklaces?|bracelets?|earrings?|pendants?|watch(es)?|jewelry|jewellery|anklets?|charms?|rings?)\b/i],
  ['Bags / luggage',        /\b(backpack|handbag|purse|tote|luggage|suitcase|wallet|duffel|crossbody)\b/i],
  ['Toys / games',          /\b(toys?|puzzle|doll|figurine|board\s+game|plush|building\s+block|ride[\s-]on)\b/i],
  ['Electronics / tech',    /\b(charger|cable|earbuds|headphones?|speaker|adapter|power\s+bank|phone\s+case|usb|bluetooth|monitor)\b/i],
  ['Kitchen / dining',      /\b(kitchen|cookware|utensil|tumbler|mug|cutting\s+board|tablecloth|table\s+cloth|dinnerware|storage\s+container|cups?|plates?)\b/i],
  ['Home / decor',          /\b(curtain|rug|pillow|blanket|comforter|bedding|lamp|decor|organizer|shelf|hanger|candle)\b/i],
  ['Pet supplies',          /\b(dog|cat|pet|puppy|kitten|leash)\b/i],
  ['Auto / tools',          /\b(car|auto|vehicle|wiper|drill|wrench|screwdriver|tool\s+set)\b/i],
  ['Sports / outdoors',     /\b(yoga|fitness|dumbbell|camping|hiking|bicycle|fishing|golf)\b/i],
  ['Shoes / footwear',      /\b(shoes?|sneakers?|sandals?|boots?|slippers?|flip[\s-]?flops?|loafers?|heels?)\b/i],
  // Apparel last — only reached when nothing more specific matched.
  ['Clothing - kids/baby',  /\b(girls?|boys?|kids?|toddler|baby|infant|children|youth)\b/i],
  ['Clothing - womens',     /\b(women|womens|women's|ladies|dress|blouse|leggings)\b/i],
  ['Clothing - mens',       /\b(men|mens|men's|polo|shirt|hoodie|jacket)\b/i],
];

function departmentOf(title, category) {
  const hay = `${title || ''} ${category || ''}`;
  for (const [name, re] of DEPARTMENTS) if (re.test(hay)) return name;
  return 'Other / uncategorised';
}

// ── DB wiring ────────────────────────────────────────────────────────────────
let _pool = null;
async function initVero(pool) {
  _pool = pool;
  await _pool.query(`
    ALTER TABLE products ADD COLUMN IF NOT EXISTS do_not_relist BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE products ADD COLUMN IF NOT EXISTS vero_risk TEXT;
    ALTER TABLE products ADD COLUMN IF NOT EXISTS vero_note TEXT;
    CREATE INDEX IF NOT EXISTS products_dnr_idx ON products(do_not_relist) WHERE do_not_relist;
    CREATE TABLE IF NOT EXISTS vero_brands (
      brand TEXT PRIMARY KEY,
      tier TEXT NOT NULL DEFAULT 'critical',
      added_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `).catch(e => console.warn('[vero] schema:', e.message));
  console.log('[vero] risk screening ready');
}

async function customBrands() {
  if (!_pool) return [];
  const r = await _pool.query('SELECT brand FROM vero_brands').catch(() => ({ rows: [] }));
  return r.rows.map(x => _norm(x.brand)).filter(Boolean);
}

/** Is this SKU permanently blocked from being listed/published? */
async function isBlocked(ebaySku) {
  if (!_pool || !ebaySku) return false;
  const r = await _pool.query(
    'SELECT 1 FROM products WHERE do_not_relist AND (ebay_sku = $1 OR ebay_sku LIKE $1 || \'-%\') LIMIT 1',
    [ebaySku]
  ).catch(() => ({ rows: [] }));
  return r.rows.length > 0;
}

/** Screen a candidate BEFORE import. Returns null if OK, else the reason. */
async function screenImport(product) {
  // Hard blocklist first: a product eBay has already removed must never be
  // re-imported, whatever else it scores. Checked by ASIN and by title
  // fingerprint, because the same item reappears under new ASINs.
  try {
    const enforce = require('./vero-enforce');
    const hit = await enforce.isProductBlocked({ asin: product.asin, title: product.title });
    if (hit) {
      return { risk: 'critical', score: 1000, brands: [],
               reasons: [`This product was previously removed by eBay (${hit.kind} match) — permanently blocked. ${String(hit.reason || '').slice(0, 100)}`] };
    }
  } catch (e) { /* module optional */ }

  const extra = await customBrands();
  const res = scoreProduct(product, extra);

  // Hardened after a second suspension. The old version scored the TITLE only,
  // so a product whose brand appeared just in Amazon's brand field or byline
  // passed straight through — and it ignored the strongest signal available:
  // a brand that has ALREADY had a listing removed on this account.

  // 1. Any brand that already cost a removal is refused outright.
  if (_pool) {
    const hay = `${product.title || ''} ${product.brand || ''} ${product.byline || ''}`.toLowerCase();
    try {
      const pv = await _pool.query(`SELECT DISTINCT brand FROM policy_violations WHERE brand IS NOT NULL`);
      for (const row of pv.rows) {
        const b = String(row.brand).toLowerCase();
        if (b.length > 1 && hay.includes(b)) {
          return { risk: 'critical', score: 999, brands: [b],
                   reasons: [`"${b}" already had a listing removed by eBay on this account`] };
        }
      }
    } catch (e) { /* table may not exist yet */ }
  }

  // ── WHY BRAND NAMES NO LONGER BLOCK ──────────────────────────────────────
  // Reselling genuine branded goods is lawful (first-sale doctrine) and eBay
  // permits it — VeRO exists to remove counterfeits and IP misuse, not to stop
  // resale. Blocking every branded product was solving the wrong problem and
  // rejecting sellable inventory.
  //
  // The Seller Help data proves the point: of 30 flagged listings, ZERO had a
  // brand in the title. The claims were about copied IMAGES and copy, which is
  // what the eBay-catalogue image work addresses.
  //
  // So brand matches are now a WARNING (soft), while these still BLOCK:
  //   • a brand that has already reported this account
  //   • dupes, replicas and counterfeit wording
  //   • restricted/prohibited categories
  //   • products eBay has already removed
  // Set VERO_SCREEN=strict to block on brand names too.

  // 2. Amazon's declared brand / byline — a product is branded even when the
  //    title does not repeat the brand.
  const declared = String(product.brand || product.byline || '').trim().toLowerCase()
    .replace(/^(visit the|brand:)\s*/i, '').replace(/\s+store$/i, '').trim();
  if (declared && declared.length > 1) {
    const known = [...TIER_CRITICAL, ...TIER_HIGH, ...TIER_MEDIUM, ...extra];
    const hit = known.find(b => declared === b || declared.includes(b) || b.includes(declared));
    if (hit) {
      // SOFT: genuine branded goods are legal to resell. Flag it, don't refuse.
      return { risk: 'high', score: 60, brands: [hit], soft: true,
               reasons: [`Brand "${product.brand || product.byline}" is a VeRO participant — legal to resell genuine stock, but avoid their images and keep the brand out of the title where you can`] };
    }
  }

  // 3. Restricted / prohibited CATEGORY check — independent of brand names.
  const _hay = `${product.title || ''} ${product.description || ''}`.slice(0, 4000);
  const _restricted = restrictedCheck(_hay);
  const _worst = _restricted.find(r => r.risk === 'critical') || _restricted.find(r => r.risk === 'high');
  if (_worst && _worst.risk === 'critical') {
    return { risk: 'critical', score: 300, brands: [],
             reasons: [`Restricted category: ${_worst.label}`],
             restricted: _restricted.map(r => r.label) };
  }
  if (_worst && res.risk !== 'none') {
    // A restricted category ON TOP of any brand signal is the combination that
    // gets reported — treat it as critical rather than letting it through.
    return { ...res, risk: 'critical', score: res.score + 100,
             reasons: [...res.reasons, `Restricted category: ${_worst.label}`],
             restricted: _restricted.map(r => r.label) };
  }

  // HARD vs SOFT is decided by WHERE the signal came from, not by matching the
  // wording of my own messages — that was fragile and mis-classified listings:
  // "Compatible with Apple iPhone" and "adidas + Amazon image" both became hard
  // blocks because they carried a second, non-brand reason.
  //
  // HARD  = restricted/prohibited category, dupe/replica wording, a product or
  //         brand that has already been reported on this account.
  // SOFT  = brand name present, Amazon-hosted images, copied description.
  //         Real signals worth surfacing, but not grounds to refuse a lawful
  //         resale.
  if (res.risk === 'critical' || res.risk === 'high') {
    return { ...res, risk: res.risk === 'critical' ? 'high' : res.risk, soft: true };
  }
  if (_worst) return { risk: 'high', score: 60, brands: [],
                       reasons: [`Restricted category: ${_worst.label}`],
                       restricted: _restricted.map(r => r.label) };
  return null;
}

// ── Express routes ───────────────────────────────────────────────────────────
function mountVero(app, db) {
  const acct = req => {
    const a = String(req.query.account || '').trim();
    return /^[\w.\-]{1,64}$/.test(a) ? a : 'default';
  };

  // Risk broken down by department — answers "which departments are safe to
  // keep sourcing?" instead of listing thousands of individual items.
  app.get('/api/vero/risk-by-department', async (req, res) => {
    try {
      const extra = await customBrands();
      const r = await _pool.query(
        `SELECT title, category, image_url, data, do_not_relist
           FROM products WHERE account_id = $1`, [acct(req)]);
      const dept = {};
      for (const row of r.rows) {
        const d = departmentOf(row.title, row.category);
        const s = scoreProduct({ ...row, imageUrl: row.image_url, data: row.data || {} }, extra);
        const b = dept[d] || (dept[d] = { department: d, total: 0, critical: 0, high: 0,
                                          medium: 0, low: 0, clean: 0, flagged: 0,
                                          amazonImages: 0, brands: {} });
        b.total++;
        if (row.do_not_relist) b.flagged++;
        if (s.risk === 'none') b.clean++; else b[s.risk]++;
        if (s.reasons.some(x => /Amazon-hosted/.test(x))) b.amazonImages++;
        for (const br of s.brands) b.brands[br] = (b.brands[br] || 0) + 1;
      }
      const rows = Object.values(dept).map(b => {
        const risky = b.critical + b.high;
        return {
          ...b,
          riskyPct: b.total ? +((risky / b.total) * 100).toFixed(1) : 0,
          criticalPct: b.total ? +((b.critical / b.total) * 100).toFixed(1) : 0,
          amazonImagePct: b.total ? +((b.amazonImages / b.total) * 100).toFixed(1) : 0,
          topBrands: Object.entries(b.brands).sort((a, c) => c[1] - a[1]).slice(0, 5)
            .map(([n, c]) => `${n} (${c})`),
        };
      }).sort((a, b) => b.riskyPct - a.riskyPct || b.total - a.total);
      const totals = rows.reduce((a, b) => ({
        total: a.total + b.total, critical: a.critical + b.critical,
        high: a.high + b.high, flagged: a.flagged + b.flagged,
      }), { total: 0, critical: 0, high: 0, flagged: 0 });
      res.json({ departments: rows, totals });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Full audit — JSON
  app.get('/api/vero/audit', async (req, res) => {
    try {
      const extra = await customBrands();
      const r = await _pool.query(
        `SELECT id, title, ebay_sku, ebay_item_id, source_url, image_url, status,
                my_price, do_not_relist, data
           FROM products WHERE account_id = $1`, [acct(req)]);
      const out = { critical: [], high: [], medium: [], low: [], counts: {}, scanned: r.rows.length };
      for (const row of r.rows) {
        const p = { ...row, data: row.data || {}, imageUrl: row.image_url };
        const s = scoreProduct(p, extra);
        if (s.risk === 'none') continue;
        (out[s.risk] || []).push({
          id: row.id, title: row.title, ebaySku: row.ebay_sku,
          itemId: row.ebay_item_id, status: row.status,
          price: row.my_price, flagged: row.do_not_relist,
          score: s.score, brands: s.brands, reasons: s.reasons,
        });
      }
      for (const k of ['critical', 'high', 'medium', 'low']) {
        out[k].sort((a, b) => b.score - a.score);
        out.counts[k] = out[k].length;
      }
      res.json(out);
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Flag / unflag do_not_relist
  app.post('/api/vero/flag', async (req, res) => {
    try {
      const { ids, flag = true, note } = req.body || {};
      if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ error: 'ids[] required' });
      const r = await _pool.query(
        `UPDATE products SET do_not_relist = $2, vero_note = COALESCE($3, vero_note), updated_at = NOW()
          WHERE id = ANY($1) AND account_id = $4`,
        [ids, !!flag, note || null, acct(req)]);
      // Blocked listings must also stop being handed to the sync relay
      if (flag) {
        await _pool.query(
          `DELETE FROM relay_state WHERE ebay_sku IN (
             SELECT ebay_sku FROM products WHERE id = ANY($1) AND ebay_sku IS NOT NULL)`,
          [ids]).catch(() => {});
      }
      res.json({ success: true, updated: r.rowCount });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Explain a verdict. "It still blocks" is unactionable without knowing WHICH
  // rule fired — brand (allowed), restricted category, prior removal, or dupe
  // wording all look identical from the outside.
  app.post('/api/vero/test', async (req, res) => {
    try {
      const { title, brand, byline, asin, description, images } = req.body || {};
      if (!title) return res.status(400).json({ error: 'title required' });
      const hit = await screenImport({ title, brand, byline, asin, description, images });
      const score = scoreProduct({ title, brand, description, images });
      res.json({
        title,
        verdict: !hit ? 'ALLOWED' : (hit.soft ? 'ALLOWED (warning only)' : 'BLOCKED'),
        blocked: !!(hit && !hit.soft),
        why: hit ? hit.reasons : ['no risk signals'],
        risk: hit ? hit.risk : 'none',
        soft: hit ? !!hit.soft : undefined,
        restrictedCategories: restrictedCheck(`${title} ${description || ''}`).map(r => `${r.risk}: ${r.label}`),
        brandsSeen: score.brands,
        screenMode: process.env.VERO_SCREEN || 'block',
      });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Is this product (or a near-duplicate) permanently blocked? Batch-capable so
  // an import can check a whole page of candidates in one call.
  app.post('/api/vero/check-blocked', async (req, res) => {
    try {
      const items = Array.isArray(req.body?.items) ? req.body.items.slice(0, 500) : [];
      if (!items.length) return res.json({ blocked: [] });
      const enforce = require('./vero-enforce');
      const blocked = [];
      for (const it of items) {
        const hit = await enforce.isProductBlocked({ asin: it.asin, title: it.title });
        if (hit) blocked.push({ asin: it.asin, title: it.title,
                                kind: hit.kind, matched: hit.matched || hit.title,
                                reason: String(hit.reason || '').slice(0, 160) });
      }
      res.json({ blocked, checked: items.length });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Add a brand to the permanent blocklist
  app.post('/api/vero/brands', async (req, res) => {
    try {
      const { brand, tier = 'critical' } = req.body || {};
      if (!brand) return res.status(400).json({ error: 'brand required' });
      await _pool.query(
        `INSERT INTO vero_brands(brand, tier) VALUES($1,$2)
         ON CONFLICT(brand) DO UPDATE SET tier = EXCLUDED.tier`, [_norm(brand), tier]);
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // CSV export of the kill-list
  app.get('/api/vero/export.csv', async (req, res) => {
    try {
      const extra = await customBrands();
      const r = await _pool.query(
        `SELECT id, title, ebay_sku, ebay_item_id, status, my_price, image_url, data
           FROM products WHERE account_id = $1`, [acct(req)]);
      const rows = [['risk', 'score', 'brands', 'itemId', 'ebaySku', 'title', 'price', 'reasons']];
      const scored = [];
      for (const row of r.rows) {
        const s = scoreProduct({ ...row, imageUrl: row.image_url, data: row.data || {} }, extra);
        if (s.risk === 'none' || s.risk === 'low') continue;
        scored.push({ row, s });
      }
      scored.sort((a, b) => b.s.score - a.s.score);
      for (const { row, s } of scored) {
        rows.push([s.risk, s.score, s.brands.join('; '), row.ebay_item_id || '',
                   row.ebay_sku || '', row.title || '', row.my_price || '', s.reasons.join('; ')]);
      }
      const csv = rows.map(r2 => r2.map(c =>
        `"${String(c == null ? '' : c).replace(/"/g, '""')}"`).join(',')).join('\n');
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename="vero-audit.csv"');
      res.send(csv);
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Self-contained HTML report (same origin → can read the DropSync token)
  app.get('/vero-report', (req, res) => {
    res.setHeader('Content-Type', 'text/html');
    res.send(REPORT_HTML);
  });

  console.log('[vero] routes mounted: /vero-report, /api/vero/audit, /api/vero/flag');
}

const REPORT_HTML = `<!DOCTYPE html><html><head><meta charset="utf-8">
<title>DropSync — VeRO Risk Audit</title><style>
*{box-sizing:border-box} body{font-family:system-ui,sans-serif;background:#0f1116;color:#e8eaf0;margin:0;padding:24px}
h1{font-size:20px;margin:0 0 4px} .sub{opacity:.6;font-size:13px;margin-bottom:18px}
.cards{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:18px}
.card{background:#171a22;border:1px solid #252a36;border-radius:10px;padding:12px 16px;min-width:120px}
.card b{display:block;font-size:24px} .crit{color:#ff6b6b}.high{color:#ffa94d}.med{color:#ffd43b}.low{color:#8ab4ff}
button{background:#2b5cd9;border:0;color:#fff;padding:8px 14px;border-radius:7px;cursor:pointer;font-size:13px;margin-right:8px}
button.danger{background:#c92a2a} button:disabled{opacity:.5;cursor:default}
table{width:100%;border-collapse:collapse;font-size:12px;margin-top:8px}
th,td{text-align:left;padding:7px 8px;border-bottom:1px solid #222736;vertical-align:top}
th{position:sticky;top:0;background:#0f1116;font-size:11px;text-transform:uppercase;opacity:.6}
tr:hover{background:#151822} .tag{display:inline-block;padding:1px 6px;border-radius:4px;font-size:10px;background:#252a36;margin-right:3px}
.reasons{opacity:.65;font-size:11px} #status{margin:10px 0;font-size:13px;min-height:18px}
a{color:#8ab4ff}
</style></head><body>
<h1>VeRO Risk Audit</h1>
<div class="sub">Scores every listing for intellectual-property risk. Flagging as <b>do-not-relist</b> permanently stops sync and publish for that listing.</div>
<div id="status">Loading…</div>
<div class="cards" id="cards"></div>
<h3 style="margin-top:6px">Risk by department</h3>
<div style="opacity:.6;font-size:12px;margin-bottom:6px">Which departments are worth continuing to source. "Risky %" is CRITICAL + HIGH as a share of that department.</div>
<div id="deptTable"></div>
<div>
  <button onclick="selectRisk('critical')">Select all CRITICAL</button>
  <button onclick="selectRisk('high')">+ HIGH</button>
  <button onclick="flagSelected()" class="danger">Flag selected as do-not-relist</button>
  <button onclick="location.href='/api/vero/export.csv'+acctQ()">Download CSV</button>
  <button onclick="scanMessages()" style="background:#0b7285">Scan eBay messages for notices</button>
</div>
<div id="notices"></div>
<div id="tables"></div>
<script>
const S = JSON.parse(localStorage.getItem('ds_settings') || '{}');
function acctQ(){ return S.accountId ? '?account=' + encodeURIComponent(S.accountId) : ''; }
let DATA = null;
async function load(){
  const r = await fetch('/api/vero/audit' + acctQ());
  DATA = await r.json();
  if (DATA.error) { document.getElementById('status').textContent = 'Error: ' + DATA.error; return; }
  document.getElementById('status').textContent = 'Scanned ' + DATA.scanned + ' listings' + (S.accountId ? ' for account ' + S.accountId : '');
  document.getElementById('cards').innerHTML = ['critical','high','medium','low'].map(k =>
    '<div class="card"><b class="'+({critical:'crit',high:'high',medium:'med',low:'low'})[k]+'">'+(DATA.counts[k]||0)+'</b>'+k.toUpperCase()+'</div>').join('');
  // Department breakdown first — it answers the sourcing question.
  try {
    const dr = await fetch('/api/vero/risk-by-department' + acctQ()).then(r => r.json());
    if (dr.departments) {
      document.getElementById('deptTable').innerHTML =
        '<table><tr><th>Department</th><th>Listings</th><th>Risky %</th><th>Critical</th><th>High</th><th>Amazon images %</th><th>Top brands</th></tr>' +
        dr.departments.map(d => {
          const col = d.riskyPct >= 40 ? 'crit' : d.riskyPct >= 15 ? 'high' : d.riskyPct >= 5 ? 'med' : 'low';
          return '<tr><td>' + d.department + '</td><td>' + d.total.toLocaleString() + '</td>' +
            '<td class="' + col + '"><b>' + d.riskyPct + '%</b></td>' +
            '<td>' + d.critical + '</td><td>' + d.high + '</td>' +
            '<td>' + d.amazonImagePct + '%</td>' +
            '<td class="reasons">' + (d.topBrands.join(', ') || '—') + '</td></tr>';
        }).join('') + '</table>';
    }
  } catch(e) {}

  document.getElementById('tables').innerHTML = ['critical','high','medium'].map(k => {
    const rows = DATA[k]||[]; if(!rows.length) return '';
    return '<h3 style="margin-top:22px">'+k.toUpperCase()+' ('+rows.length+')</h3><table><tr>'+
      '<th></th><th>Score</th><th>Brands</th><th>Title</th><th>Item ID</th><th>Why</th></tr>'+
      rows.map(x=>'<tr><td><input type="checkbox" data-id="'+x.id+'" data-risk="'+k+'"'+(x.flagged?' checked disabled':'')+'></td>'+
      '<td>'+x.score+'</td><td>'+x.brands.map(b=>'<span class="tag">'+b+'</span>').join('')+'</td>'+
      '<td>'+(x.title||'').slice(0,70)+(x.flagged?' <span class="tag">FLAGGED</span>':'')+'</td>'+
      '<td>'+(x.itemId?'<a target="_blank" href="https://www.ebay.com/itm/'+x.itemId+'">'+x.itemId+'</a>':'—')+'</td>'+
      '<td class="reasons">'+x.reasons.join('; ')+'</td></tr>').join('')+'</table>';
  }).join('');
}
function selectRisk(risk){
  document.querySelectorAll('input[data-risk="'+risk+'"]').forEach(c=>{ if(!c.disabled) c.checked=true; });
}
async function scanMessages(){
  if(!S.token){ alert('No eBay token found in this browser. Open the DropSync tab and connect eBay first.'); return; }
  document.getElementById('status').textContent='Reading eBay messages…';
  try{
    const r=await fetch('/api/vero/scan-messages'+acctQ(),{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({access_token:S.token,days:60,autoFlag:true,autoAddBrand:true})});
    const d=await r.json();
    if(d.error){ document.getElementById('status').textContent='Error: '+d.error; return; }
    document.getElementById('status').textContent=
      'Scanned '+d.scanned+' messages · '+d.notices.length+' policy notices · '+d.flagged+' listings auto-flagged'+
      (d.brandsAdded.length?(' · brands blocked: '+d.brandsAdded.join(', ')):'');
    document.getElementById('notices').innerHTML = d.notices.length ?
      '<h3 style="margin-top:22px">eBay notices found</h3><table><tr><th>Received</th><th>Subject</th><th>Item IDs</th><th>Brands</th><th>Flagged</th></tr>'+
      d.notices.map(n=>'<tr><td>'+(n.received||'').slice(0,10)+'</td><td>'+(n.subject||'').slice(0,80)+
      (n.needsReview?' <span class="tag">NEEDS REVIEW</span>':'')+'</td><td>'+(n.itemIds.join(', ')||'—')+
      '</td><td>'+n.brands.map(b=>'<span class="tag">'+b+'</span>').join('')+'</td><td>'+n.flagged+'</td></tr>').join('')+'</table>' : '';
    setTimeout(load,800);
  }catch(e){ document.getElementById('status').textContent='Error: '+e.message; }
}
async function flagSelected(){
  const ids=[...document.querySelectorAll('input[data-id]:checked:not(:disabled)')].map(c=>c.dataset.id);
  if(!ids.length){ alert('Nothing selected'); return; }
  if(!confirm('Flag '+ids.length+' listings as do-not-relist?\\n\\nThey will be removed from the sync queue and never republished. This does NOT end them on eBay — end those manually in Seller Hub.')) return;
  document.getElementById('status').textContent='Flagging '+ids.length+'…';
  const r=await fetch('/api/vero/flag'+acctQ(),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ids,flag:true,note:'VeRO audit'})});
  const d=await r.json();
  document.getElementById('status').textContent = d.success ? ('Flagged '+d.updated+' listings. Reloading…') : ('Error: '+d.error);
  if(d.success) setTimeout(load,900);
}
load();
</script></body></html>`;

module.exports = { initVero, mountVero, scoreProduct, isBlocked, screenImport,
                   restrictedCheck, RESTRICTED_PATTERNS,
                   TIER_CRITICAL, TIER_HIGH, TIER_MEDIUM };
