// DropSync eBay handler — runs on Railway Express server
// Polyfill fetch with node-fetch if global fetch is not available (Node < 18)
if (typeof fetch === 'undefined') {
  const _nf = require('node-fetch');
  global.fetch = _nf.default || _nf;
}

// DropSync AI Agent — Amazon → eBay Dropshipping Backend
// Clean architecture: per-ASIN prices+images, AI category detection, auto policies

const SCOPES = 'https://api.ebay.com/oauth/api_scope https://api.ebay.com/oauth/api_scope/sell.inventory https://api.ebay.com/oauth/api_scope/sell.account https://api.ebay.com/oauth/api_scope/sell.fulfillment https://api.ebay.com/oauth/api_scope/sell.marketing';

function getEbayUrls() {
  return {
    EBAY_API:     'https://api.ebay.com',
    EBAY_AUTH:    'https://auth.ebay.com/oauth2/authorize',
    EBAY_TOK:     'https://api.ebay.com/identity/v1/oauth2/token',
    EBAY_TRADING: 'https://api.ebay.com/ws/api.dll',
    CLIENT_ID:    process.env.EBAY_CLIENT_ID,
    CLIENT_SECRET:process.env.EBAY_CLIENT_SECRET,
    REDIRECT:     process.env.EBAY_REDIRECT_URI,
  };
}

const UA_LIST = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
];
const randUA = () => UA_LIST[Math.floor(Math.random() * UA_LIST.length)];

// ── Amazon page fetcher: direct + proxy fallbacks ─────────────────────────────
// Module-level handle to the browser relay (set by server.js at boot via
// setRelayHandle). When the browser tab is alive, fetchPage tries the relay
// first for amazon.com URLs — residential IP + real cookies = ~95% success
// vs ~25% from Railway IP. Falls back to direct fetch on relay timeout/miss.
let _relayHandle = null; // { isAlive: () => bool, db: { enqueueRelayFetch, awaitRelayResult } }
function setRelayHandle(h) { _relayHandle = h; }

// ── Server-side direct-fetch health tracking ─────────────────────────────────
// Rolling window of recent direct-fetch outcomes (true = blocked, false = ok).
// When the block rate crosses the threshold, fetchPage puts the whole pipeline
// into a global backoff for a fixed duration. First successful fetch during
// backoff clears it.
let _recentBlocks = [];
let _backoffUntil = 0;
function _recordDirectResult(blocked) {
  _recentBlocks.push(blocked);
  if (_recentBlocks.length > 20) _recentBlocks.shift(); // keep window bounded
}
function _maybeTriggerBackoff(windowSize, threshold, durationMs) {
  if (_recentBlocks.length < windowSize) return;
  const lastN = _recentBlocks.slice(-windowSize);
  const blocked = lastN.filter(Boolean).length;
  if (blocked >= threshold && _backoffUntil < Date.now()) {
    _backoffUntil = Date.now() + durationMs;
    console.warn(`[fetch] ⚠️ backoff triggered: ${blocked}/${windowSize} recent direct fetches blocked — pausing ${Math.round(durationMs/1000)}s`);
    _recentBlocks = []; // reset window so we don't re-trigger immediately
  }
}
function _clearBackoffOnSuccess() {
  if (_backoffUntil > Date.now()) {
    console.log(`[fetch] ✓ direct fetch succeeded — clearing backoff early`);
    _backoffUntil = 0;
  }
}

async function fetchPage(url, ua, maxAttempts) {
  const isBlocked = (html) => {
    if (!html || html.length < 1000) return true;
    if (html.includes('Type the characters')) return true;
    if (html.includes('robot check')) return true;
    if (html.includes('Enter the characters')) return true;
    if (html.includes('automated access')) return true;
    if (html.includes('api-services-support@amazon.com')) return true;
    if (html.includes('validateCaptcha')) return true;
    if (html.includes('auth-captcha')) return true;
    if (html.includes('sign in to continue')) return true;
    if (html.length < 10000 && !html.includes('productTitle') && !html.includes('asin')) return true;
    return false;
  };

  const asin = url.match(/\/dp\/([A-Z0-9]{10})/)?.[1];
  const _asinTag = asin ? ` [${asin}]` : '';

  // ── RELAY FIRST: try browser-fetched HTML before burning a direct attempt ──
  // Only for amazon.com URLs (the only domain that gets blocked) and only
  // when a browser tab is actively heartbeating. The relay enqueues the URL,
  // a browser tab claims it, fetches Amazon from a residential IP, POSTs
  // the HTML back. We await up to 60s. If the relay times out or returns
  // blocked HTML, we fall through to the existing direct-fetch logic below.
  if (_relayHandle && _relayHandle.isAlive() && /amazon\.(com|co\.uk|de|ca)/.test(url)) {
    try {
      const jobId = await _relayHandle.db.enqueueRelayFetch(url, asin);
      const html = await _relayHandle.db.awaitRelayResult(jobId, 60000);
      if (html && !isBlocked(html)) {
        console.log(`[fetch] relay hit${_asinTag} (${html.length}b)`);
        return html;
      }
      if (html) console.log(`[fetch] relay returned blocked HTML${_asinTag} — falling back to direct`);
      else      console.log(`[fetch] relay timeout${_asinTag} — falling back to direct`);
    } catch(e) {
      console.warn(`[fetch] relay error${_asinTag}: ${e.message} — falling back to direct`);
    }
  }

  // Vary Accept-Language + platform per request to avoid uniform fingerprint
  const _langList = ['en-US,en;q=0.9', 'en-US,en;q=0.8,fr;q=0.5', 'en-GB,en;q=0.9', 'en-US,en;q=0.9,es;q=0.7', 'en-US,en;q=0.9,de;q=0.5'];
  const _platformList = ['"Windows"', '"macOS"', '"Linux"', '"Windows"', '"Windows"'];
  const _langIdx = Math.floor(Math.random() * _langList.length);
  const _lang = _langList[_langIdx];
  const _platform = _platformList[_langIdx];
  // Rotate Chrome versions — identical version on every request is a bot signal
  const _chromeVersions = ['131', '132', '133', '134', '135'];
  const _cv = _chromeVersions[Math.floor(Math.random() * _chromeVersions.length)];

  // Serverless CDN IPs rotate per invocation (residential-grade).
  // Use varied UA + headers + URL forms + delays to maximize hit rate.
  const urlVariants = asin ? [
    `https://www.amazon.com/dp/${asin}?th=1&psc=1`,
    `https://www.amazon.com/dp/${asin}`,
    `https://www.amazon.com/dp/${asin}?psc=1`,
    `https://www.amazon.com/dp/${asin}?th=1`,
    url,
  ] : [url];

  const referers = [
    null,
    'https://www.google.com/',
    'https://www.amazon.com/',
    'https://www.google.com/search?q=amazon+' + (asin||'product'),
    'https://www.bing.com/search?q=amazon',
  ];

  const makeHeaders = (agent, referer, extra) => ({
    'User-Agent': agent || ua || randUA(),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': _lang,
    'Accept-Encoding': 'gzip, deflate, br',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': referer ? 'cross-site' : 'none',
    'Sec-Fetch-User': '?1',
    'Sec-CH-UA': `"Google Chrome";v="${_cv}", "Chromium";v="${_cv}", "Not_A Brand";v="24"`,
    'Sec-CH-UA-Mobile': '?0',
    'Sec-CH-UA-Platform': _platform,
    'Sec-CH-UA-Platform-Version': '"15.0"',
    'Sec-CH-UA-Arch': '"x86"',
    'Sec-CH-UA-Bitness': '"64"',
    'Viewport-Width': String(1280 + Math.floor(Math.random() * 640)),
    'Device-Memory': ['4', '8', '16'][Math.floor(Math.random() * 3)],
    ...(referer ? { 'Referer': referer } : {}),
    ...(extra || {}),
  });

  // ── SERVER DIRECT FETCH: delayed + back-off logic ───────────────────────────
  // Tweakable knobs (change these to tune behavior):
  const DIRECT_UPFRONT_DELAY_MS = 3000; // pause before first direct attempt
  const BLOCK_RATE_WINDOW       = 10;   // track last N direct attempts
  const BLOCK_RATE_THRESHOLD    = 9;    // if ≥ N of last 10 got blocked → back off
  const BACKOFF_DURATION_MS     = 60000;// how long to pause the whole pipeline

  // Upfront delay: relay-fallback arrivals land here, so give the IP a breather
  // before a first attempt. Helps avoid immediate block cascades.
  await sleep(DIRECT_UPFRONT_DELAY_MS);

  // Global backoff check — if recent direct fetches have been mostly blocked,
  // Amazon has probably flagged the Railway IP. Pause the server-fetch pipeline
  // for BACKOFF_DURATION_MS before any new direct attempts.
  if (_backoffUntil > Date.now()) {
    const _remaining = Math.round((_backoffUntil - Date.now()) / 1000);
    console.log(`[fetch] in global backoff${_asinTag} — ${_remaining}s left, skipping direct`);
    return '';
  }

  // Direct attempts with increasing delays and varied URL/UA/referer combos
  // maxAttempts=1 for ASIN batch fetches: fail fast, use size-price fallback instead of wasting budget
  const _maxAttempts = maxAttempts || 5;
  const delays = [0, 1000, 2500, 5000, 10000];
  for (let i = 0; i < _maxAttempts; i++) {
    if (i > 0) await sleep(delays[i]);
    try {
      const uaIdx = (Date.now() + i * 7) % UA_LIST.length;
      const tryUrl = urlVariants[i % urlVariants.length];
      const referer = referers[i % referers.length];
      const extra = i === 3 ? { 'Cookie': 'session-id=000-0000000-0000000; i18n-prefs=USD; lc-main=en_US' }
                  : i === 4 ? { 'Cookie': 'i18n-prefs=USD', 'X-Forwarded-For': '' }
                  : undefined;
      const r = await fetch(tryUrl, {
        headers: makeHeaders(UA_LIST[uaIdx|0], referer, extra),
        redirect: 'follow',
      });
      const html = await r.text();
      if (!isBlocked(html)) {
        // ok fetch logged by caller's batch summary — suppress here to save log lines
        _recordDirectResult(false);
        _clearBackoffOnSuccess();
        return html;
      }
      console.warn(`[fetch] direct attempt ${i+1} blocked (${html.length}b)${_asinTag}`);
      _recordDirectResult(true);
      _maybeTriggerBackoff(BLOCK_RATE_WINDOW, BLOCK_RATE_THRESHOLD, BACKOFF_DURATION_MS);
    } catch(e) {
      console.warn(`[fetch] direct attempt ${i+1} error: ${e.message}${_asinTag}`);
      _recordDirectResult(true);
      _maybeTriggerBackoff(BLOCK_RATE_WINDOW, BLOCK_RATE_THRESHOLD, BACKOFF_DURATION_MS);
    }
  }

  console.warn(`[fetch] ALL strategies failed${_asinTag} — qty=0 (no price data)`);
  return '';
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── Amazon OOS detection — checks all signals ─────────────────────────────────
// Amazon uses multiple OOS patterns depending on product/region/seller type
function isAmazonOOS(buyBoxHtml) {
  if (!buyBoxHtml || buyBoxHtml.length < 10) return false;
  const t = buyBoxHtml;
  const tl = t.toLowerCase();
  // outOfStockBuyBox removed: it appears for sibling variants too, use text signals only
  if (tl.includes('currently unavailable')) return true;        // #2: case-insensitive
  if (t.includes('Currently unavailable')) return true;         // #3: exact Amazon casing
  if (tl.includes('out of stock')) return true;                 // #4: generic (covers "Temporarily out of stock")
  if (tl.includes('temporarily out of stock')) return true;     // #5: explicit temp OOS
  if (t.includes('Join the waitlist')) return true;             // #6: waitlist = OOS
  // REMOVED rule #7 ("see all available options"): this string appears in
  // Amazon's standard variant-picker UI on most multi-variant listings,
  // even when the buy-box variant is fully in stock. Matching it caused
  // every multi-variant product to be flagged OOS → qty=0 on eBay despite
  // sync correctly extracting prices. Use the absence-of-add-to-cart
  // signal at rule #8 instead, which is much more specific.
  if (tl.includes('see all buying options') && !t.includes('add-to-cart-button')) return true; // #8: no direct buy = OOS
  return false;
}

// ── ScraperAPI structured Amazon product endpoint ─────────────────────────────
// Returns clean JSON with title, price, variants, images, availability
// 25 credits per call — much more reliable than HTML scraping
async function fetchAmazonStructured(asin, apiKey) {
  if (!apiKey) return null;
  const url = `https://api.scraperapi.com/structured/amazon/product?api_key=${apiKey}&asin=${asin}&country=us`;
  try {
    const r = await fetch(url, { headers: { 'Accept': 'application/json' } });
    if (!r.ok) { console.warn(`[structured] ${asin}: HTTP ${r.status}`); return null; }
    const d = await r.json();
    if (!d || (!d.name && !d.product_name && !d.title)) { console.warn(`[structured] ${asin}: empty response`); return null; }
    console.log(`[structured] ${asin}: ok — "${(d.name||d.product_name||d.title||'').slice(0,40)}" $${d.price||d.current_price||'?'}`);
    return d;
  } catch(e) {
    console.warn(`[structured] ${asin}: ${e.message}`);
    return null;
  }
}

// Parse ScraperAPI structured response into our internal format
// Returns: { price, inStock, title, images, variants: [{asin, price, inStock, dims}] }
function parseStructuredProduct(d) {
  if (!d) return null;
  const title = d.name || d.product_name || d.title || '';
  const price = parseFloat(d.price || d.current_price || d.sale_price || 0);
  const inStock = !/out.of.stock|unavailable/i.test(d.availability || '');
  const images = (d.images || d.product_photos || []).map(img => typeof img === 'string' ? img : (img.url || img.src || '')).filter(Boolean);

  // Variants array — each variant has ASIN + price + dimension values
  const variants = [];
  const rawVariants = d.variants || d.product_variants || d.product_overview?.variants || [];
  for (const v of rawVariants) {
    const vasin = v.asin || v.id || '';
    const vprice = parseFloat(v.price || v.current_price || 0);
    const vInStock = !/out.of.stock|unavailable/i.test(v.availability || v.is_available || '');
    // Dimension values: {Color: 'Red', Size: 'M'} etc.
    const dims = {};
    if (v.dimensions) {
      for (const [k, val] of Object.entries(v.dimensions)) {
        if (k && val) dims[k] = val;
      }
    }
    // Sometimes variant data has variant_attributes or selected_attributes
    const attrs = v.variant_attributes || v.selected_attributes || v.options || {};
    for (const [k, val] of Object.entries(attrs)) {
      if (k && val && !dims[k]) dims[k] = val;
    }
    if (vasin || Object.keys(dims).length > 0) {
      variants.push({ asin: vasin, price: vprice, inStock: vInStock, dims });
    }
  }

  return { title, price, inStock, images, variants };
}

// ── eBay title sanitizer — strips words that trigger policy violations ─────────
// VERO brand names to auto-strip from titles before listing on eBay
const VERO_STRIP = [
  // Major electronics & tech
  'samsung','apple','sony','microsoft','lg','bose','beats','jbl','dyson','shark',
  'roomba','ring','nest','arlo','wyze','simplisafe','logitech','razer','corsair',
  'anker','belkin','otterbox','spigen','lifeproof','fitbit','garmin','gopro',
  'nintendo','playstation','xbox','pokemon','minecraft','fortnite','lego',
  'adobe','intuit','turbotax','norton','mcafee','symantec','kaspersky',
  'epson','canon','fujifilm','panasonic','brother','xerox','dolby',
  // Footwear & apparel
  'nike','adidas','puma','reebok','under armour','new balance','vans','converse',
  'timberland','ugg','birkenstock','dr. martens','crocs','skechers',
  'spyder','quiksilver','stussy','lacoste','hugo boss','calvin klein',
  'tommy hilfiger','ralph lauren','michael kors','kate spade','coach',
  'abercrombie','american eagle','h&m',
  // Luxury fashion & jewelry
  'gucci','louis vuitton','chanel','prada','versace','burberry','rolex','omega',
  'tag heuer','cartier','hermes','dior','balenciaga','off-white','supreme','yeezy',
  'jordan brand','fendi','valentino','tiffany','vera bradley','longchamp',
  'vacheron constantin','van cleef','baume & mercier','movado',
  'mikimoto','le vian','swarovski','swiss army',
  // Outdoor & sporting
  'north face','columbia','patagonia','carhartt',
  'titleist','callaway','taylormade','spalding','shimano',
  'bushnell','leupold','vortex',
  'yeti','hydro flask','camelbak','thermos','stanley cup',
  'weber','traeger','coleman',
  // Tools & automotive
  'dewalt','milwaukee tool','makita','bosch','ryobi','craftsman',
  'black decker','stanley tools','snap-on','benchmade','buck knives','cutco',
  'john deere','harley-davidson','rolls-royce','volkswagen','jaguar','land rover','ford',
  // Home & kitchen
  'kitchenaid','cuisinart','keurig','nespresso','instant pot','vitamix',
  'ninja kitchen','breville','le creuset','calphalon','rubbermaid','bissell',
  'irobot','ergobaby','boppy','lansinoh','medela','velcro',
  // Beauty & health
  'loreal','maybelline','neutrogena','olay','cerave','clinique',
  'dyson hair','ghd','revlon','conair','wahl','gillette','oral-b','sonicare',
  'crest','colgate','shiseido','opi',
  'band-aid','pampers','huggies','tylenol','advil','pepto-bismol',
  // Entertainment & media
  'disney','marvel','dc comics','star wars','harry potter','pixar',
  'mattel','barbie','hasbro','nerf','transformers','fisher-price','tamiya',
  'wwe','playboy','warner music',
  // Sports clubs
  'manchester united','fc bayern','tottenham hotspur',
  // Audio
  'sennheiser','jabra','turtle beach','yamaha','fender','gibson',
  // Office & misc
  'sharpie','post-it','fedex','qantas','michelin','goodyear','bridgestone',
  '3m','duracell','browning','taser','oakley','ray-ban','fossil','guess',
  'nokia','motorola','subway',
];

function stripVeROFromTitle(title) {
  if (!title) return title;
  let t = title;
  // Sort longest-first so "Louis Vuitton" stripped before "Louis"
  const sorted = [...VERO_STRIP].sort((a,b) => b.length - a.length);
  for (const brand of sorted) {
    const re = new RegExp('\\b' + brand.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b', 'gi');
    t = t.replace(re, '');
  }
  // Clean leftover punctuation/spaces
  t = t.replace(/[™®©]/g, '').replace(/\s{2,}/g, ' ')
       .replace(/^[\s,\-–|:™]+|[\s,\-–|:™]+$/g, '').trim();
  return t || title;
}

// ── cleanAmazonTitle: strip variant-specific info from Amazon titles ────────
// Amazon titles often include the selected variant: "..., White/Blue, X-Large"
// or parentheticals: "...(2 Panels, 42 inches Wide by 63 inches Long, Black)"
// For multi-variant eBay listings, the title must be generic (covers all variants).
function cleanAmazonTitle(title, allDimValues) {
  if (!title) return title;
  let t = title;

  // Step 1: Remove parenthetical blocks containing dimension/qty/variant info
  // Matches: (L,31") | (2 Panels, 42 inches Wide by 63 inches Long, Black) | (Pack of 2)
  t = t.replace(/\s*\([^)]*?(?:\d+\s*(?:inch|in|cm|"|pack|panel|piece)|[A-Za-z],\s*\d|pack of \d)[^)]*?\)/gi, '');

  // Step 2: Remove actual variation values from end of title (loop until stable)
  // Uses the real dim values (colors, sizes etc) so we only remove what's actually a variant
  if (allDimValues && allDimValues.length) {
    const vals = [...allDimValues].sort((a, b) => b.length - a.length); // longest first
    let prev = '';
    while (prev !== t) {
      prev = t;
      for (const val of vals) {
        const escaped = val.replace(/[.*+?^${}()|[\]\\]/g, '\$&');
        t = t.replace(new RegExp(',\s*' + escaped + '\s*$', 'i'), '');
      }
    }
  }

  // Step 3: Remove generic trailing single color/size words not caught above
  const _genericColors = '(?:Black|White|Grey|Gray|Navy|Blue|Red|Green|Yellow|Orange|Purple|Pink|Brown|Beige|Cream|Gold|Silver|Teal|Coral|Ivory|Olive|Khaki|Burgundy|Charcoal|Taupe|Aqua|Lavender|Blush|Rosewood|Sage)';
  const _genericSizes  = '(?:One\s+Size|X?-?Small|Medium|X?-?Large|XX?-?Large|3X-?Large|4X-?Large|XS|XL|XXL|3XL|4XL)';
  let prev2 = '';
  while (prev2 !== t) {
    prev2 = t;
    t = t.replace(new RegExp(',\s*' + _genericColors + '\s*$', 'i'), '');
    t = t.replace(new RegExp(',\s*' + _genericSizes + '\s*$', 'i'), '');
  }

  // Step 4: Clean up
  t = t.replace(/[,\s\-–|:]+$/, '').replace(/\s{2,}/g, ' ').trim();
  return t;
}

// ── Strip variant-specific & brand info from title for eBay multi-variation listings ──
// Amazon titles are per-variant: "Product Name - Black, XL" or "BRAND Product Name - Red"
// eBay has ONE page for ALL variants → title must be neutral
function neutralizeTitle(title, product) {
  if (!title) return title;
  const _original = title + '';
  let t = _original;

  // 1. Strip brand name from start — handle array (Amazon aspects) or string.
  // Use fuzzy normalization so "BLACK+DECKER" matches "Black & Decker" in the title
  // (Amazon stores brand with + but writes title with & — different formatting).
  const _rawBrand = product?.aspects?.Brand || product?.aspects?.['Brand Name'] || product?.brand || '';
  const brand = (Array.isArray(_rawBrand) ? _rawBrand[0] : _rawBrand || '').trim();
  const _normForBrand = s => String(s||'').toLowerCase().replace(/[\s\+\-&]+/g, '');
  const brandNorm = _normForBrand(brand);
  if (brand && brand.length > 1 && brand.toLowerCase() !== 'unbranded') {
    // Try strict match first
    const brandEsc = brand.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const _stricMatch = t.match(new RegExp(`^${brandEsc}[\\s\\-–:,]+`, 'i'));
    if (_stricMatch) {
      t = t.slice(_stricMatch[0].length);
    } else if (brandNorm.length >= 4) {
      // Fuzzy: try matching the first 1-4 words of the title against the normalized brand
      const _ws = t.split(/\s+/);
      for (let nw = Math.min(4, _ws.length); nw >= 1; nw--) {
        const candidate = _ws.slice(0, nw).join(' ');
        if (_normForBrand(candidate) === brandNorm) {
          // Strip the candidate + any trailing separator
          t = t.slice(candidate.length).replace(/^[\s\-–:,]+/, '');
          break;
        }
      }
    }
  }

  // 1b. Auto-detect unlisted brand at start (only if aspects.Brand didn't already strip one)
  const _brandWasStripped = brand && brand.length > 1 && t !== _original;
  if (!_brandWasStripped) {
    const COMMON_STARTS = new Set(['women','womens','mens','men','girls','boys','kids','baby',
      'plus','size','new','for','the','and','one','two','set','pack','case','high','low',
      'long','short','black','white','red','blue','pink','purple','green','gray','grey','navy']);
    const _isBrandWord = w => w.length >= 3 && w.length <= 15 && /^[A-Z]/.test(w)
      && !/'s?$/.test(w) && !COMMON_STARTS.has(w.toLowerCase());
    const _ws = t.split(/\s+/);
    const _w1 = _ws[0] || '', _w2 = _ws[1] || '';
    if (_isBrandWord(_w1) && _isBrandWord(_w2)
        && !/^(Women|Womens|Mens|Men|Girls|Kids|Baby|One|Two|Plus|The|And|For|With|A-Line|Stainless|Steel)$/i.test(_w2)) {
      const _b2 = (_w1 + ' ' + _w2).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      t = t.replace(new RegExp('^' + _b2 + '[\\s\\-–:,]+'), '');
    } else if (_isBrandWord(_w1)) {
      const _b1 = _w1.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      t = t.replace(new RegExp('^' + _b1 + '[\\s\\-–:,]+'), '');
    }
  }

  // 2. Build the comprehensive strip set: every variation value from this product.
  // We use the actual variations data so we never strip a word that isn't a variant
  // for THIS product (avoids butchering "Black & Decker" when Black is a real color).
  const varValues = new Set();
  const hasColorDim = (product?.variations || []).some(vg => /color|colour/i.test(vg.name || ''));
  const hasSizeDim  = (product?.variations || []).some(vg => /size|length|width/i.test(vg.name || ''));
  const hasStyleDim = (product?.variations || []).some(vg => /style|pattern|fit/i.test(vg.name || ''));
  for (const vg of (product?.variations || [])) {
    for (const v of (vg.values || [])) {
      const val = (v.value || '').trim();
      if (!val || val.length < 2) continue;
      varValues.add(val.toLowerCase());
    }
  }

  // Helper: regex-escape a string
  const _esc = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

  // 3. Strip parenthesized chunks that contain ANY variation value or generic
  // descriptor. "(Beige, 9x12)", "(4-Pack)", "(Queen, Pink)" all gone.
  t = t.replace(/\s*[\(\[]([^\)\]]{1,80})[\)\]]/g, (full, inner) => {
    const innerLower = inner.toLowerCase();
    // Strip if any full variation value appears inside
    for (const val of varValues) {
      if (val.length >= 3 && innerLower.includes(val)) return ' ';
    }
    // Strip if it looks like a pack count, dimension, or generic color/size
    if (/\b\d+\s*[-]?\s*(pack|count|piece|pcs?|set|ct)\b/i.test(inner)) return ' ';
    if (/\b\d+(?:\.\d+)?\s*['"]?\s*[xX×]\s*\d+/.test(inner)) return ' '; // 9x12, 3.28x7.05
    if (/\b\d+(?:\.\d+)?\s*(?:inch|in|ft|cm|mm|m|"|')\b/i.test(inner)) return ' ';
    if (/\b(small|medium|large|x-?large|xl|xxl|2xl|3xl|4xl|5xl|one size|plus size|petite|tall|regular|queen|king|twin|full|cal king|california king)\b/i.test(inner)) return ' ';
    if (/\b(black|white|red|blue|green|yellow|pink|purple|orange|brown|gray|grey|beige|navy|teal|gold|silver|ivory|cream|tan|charcoal|coral|burgundy|wine|mint|lavender|nude|khaki|multicolor|floral|leopard|striped|plaid|solid|brushed|matte|satin|polished|brass|nickel|chrome|copper|bronze)\b/i.test(inner)) return ' ';
    return full; // keep — not variation-related
  });

  // 4. Strip pack-count phrases anywhere in the title.
  // Handles: "2 Pack", "10-Pack", "[2-Pack]", "Pack of 4", "Set of 6", "12 Count", "30 Pcs"
  t = t.replace(/\b\d+\s*[-]?\s*(?:pack|packs|count|counts|piece|pieces|pcs?|set|sets|ct|qty)\b/gi, ' ');
  t = t.replace(/\b(?:pack|set|box|bag|bundle|case)\s*of\s*\d+\b/gi, ' ');

  // 5. Strip dimension strings anywhere: "9x12", "60x80x12", `5"x3"`, "3.28 FT x 7.05 FT",
  // "5 Inch", "128mm", "2x3 ft", `2'x3'`. We're aggressive — these are almost always variant data.
  t = t.replace(/\b\d+(?:\.\d+)?\s*['"]?\s*[xX×]\s*\d+(?:\.\d+)?\s*['"]?(?:\s*[xX×]\s*\d+(?:\.\d+)?\s*['"]?)?(?:\s*(?:ft|in|inch|cm|mm|m))?\b/gi, ' ');
  t = t.replace(/\b\d+(?:\.\d+)?\s*(?:inch|inches|in|ft|feet|cm|mm|m)\b/gi, ' ');
  t = t.replace(/\b\d+(?:\.\d+)?\s*["']/g, ' '); // bare 5", 3'

  // 6. Strip variation values ANYWHERE in title (not just end). Word-boundary
  // matching so "Black" doesn't strip "Blackberry". Sorted by length DESC so
  // "Brushed Nickel" matches before "Nickel" alone. We strip ONLY full
  // variation values, not individual words within them — splitting "Nugget Black"
  // into "nugget" + "black" would butcher product names like "Nugget Ice Maker"
  // where the brand/product type appears at the start of every variation value.
  // The backstop in step 7 handles standalone color/material words separately.
  const _sortedVarValues = [...varValues].sort((a, b) => b.length - a.length);
  for (const val of _sortedVarValues) {
    if (val.length < 3) continue;
    const esc = _esc(val);
    t = t.replace(new RegExp('\\b' + esc + '\\b', 'gi'), ' ');
  }

  // 7. Backstop strip: standalone generic color/size/material words if the
  // product HAS that dimension. Catches values that weren't in varValues
  // because Amazon's title uses synonyms (e.g., title says "Crimson" but
  // variation list calls it "Red").
  if (hasColorDim) {
    t = t.replace(/\b(?:black|white|ivory|cream|red|crimson|scarlet|blue|navy|royal|teal|turquoise|aqua|cyan|green|olive|forest|emerald|sage|mint|lime|yellow|gold|mustard|amber|orange|coral|peach|salmon|pink|rose|fuchsia|magenta|purple|violet|lavender|lilac|plum|brown|tan|beige|nude|khaki|chocolate|brick|rust|burgundy|wine|maroon|gray|grey|charcoal|silver|chrome|nickel|brass|bronze|copper|natural|clear|translucent|multicolor|multi[- ]color|floral|leopard|zebra|striped|plaid|solid|polka dot|tie dye|camo|camouflage)\b/gi, ' ');
    t = t.replace(/\b(?:brushed|matte|satin|glossy|polished|metallic|frosted|distressed|vintage|antique)\s+(?=[a-z])/gi, ' ');
  }
  if (hasSizeDim) {
    t = t.replace(/\b(?:xxs|xs|x-?small|small|medium|large|x-?large|xl|xx-?large|xxl|2xl|3xl|4xl|5xl|6xl|one size|plus size|petite|tall|regular|queen|king|twin xl|twin|full|cal king|california king)\b/gi, ' ');
  }

  // 8. Strip year (Amazon adds "2025"/"2026" for SEO)
  t = t.replace(/\s+20(2[3-9]|[3-9]\d)\b/g, ' ');

  // 9. Cleanup: collapse spaces, fix punctuation, remove orphans
  t = t.replace(/\(\s*\)|\[\s*\]/g, ' ');         // empty parens/brackets
  // Strip orphan single chars between spaces — leftovers from stripped pack/dim
  // patterns like "1/5 Pack" → "1/ " or "3 FT x 7 FT" → " x ".
  t = t.replace(/(?:^|\s)[xX×\/](?=\s|$)/g, ' ');
  t = t.replace(/(?:^|\s)\d+\s*[\/\-]\s*(?=\s|$)/g, ' '); // "1/" or "1-" leftover
  // Strip orphan operators at very start — happens when a compound brand like
  // "Black & Decker" had "Black" stripped as a variation value, leaving "& Decker"
  t = t.replace(/^[\s,\-–|:&\+]+/, '');
  // Strip "Size" if it's left orphaned (e.g., "King Size Pillow" → "Size Pillow"
  // when King got stripped). Only strip when surrounded by other words on both sides.
  if (hasSizeDim) {
    t = t.replace(/^\s*size\s+/i, '');
    t = t.replace(/\s+size\s+/gi, ' ');
  }
  t = t.replace(/\s*,\s*,+/g, ',');               // collapse multi-commas
  t = t.replace(/\s*[-–|:]\s*[-–|:]+/g, ' - ');   // collapse multi-separators
  t = t.replace(/[,\s]+([,)\]])/g, '$1');         // ", )" → ")"
  t = t.replace(/([(\[])[,\s]+/g, '$1');          // "(, " → "("
  t = t.replace(/^[\s,\-–|:]+|[\s,\-–|:]+$/g, ''); // strip leading/trailing punct
  t = t.replace(/\s+/g, ' ').trim();

  // 10. SAFETY: if we butchered the title down to <15 chars or <3 words,
  // fall back to a less aggressive version (just brand-stripped). Better to
  // ship a noisy title than a meaningless one.
  if (t.length < 15 || t.split(/\s+/).filter(Boolean).length < 3) {
    console.warn(`[neutralizeTitle] aggressive strip left "${t}" — falling back`);
    // Fall back to just brand-strip + suffix-strip
    let fb = _original;
    if (brand && brand.length > 1 && brand.toLowerCase() !== 'unbranded') {
      fb = fb.replace(new RegExp(`^${_esc(brand)}[\\s\\-–:,]+`, 'i'), '');
    }
    fb = fb.replace(/[,\s]+[\-–|]?\s*\([^)]+\)\s*$/g, '').trim();
    fb = fb.replace(/[,\s]+[\-–|]?\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\s*$/g, '').trim();
    return fb;
  }

  return t;
}

function sanitizeTitle(title) {
  if (!title) return '';
  // Decode HTML entities
  let t = (title + "")
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(+n))
    .replace(/&amp;/gi, '&').replace(/&quot;/gi, '"')
    .replace(/&apos;/gi, "'").replace(/&nbsp;/gi, ' ')
    .replace(/& ?#? ?39 ?;? ?s\b/g, "'s")
    .trim();
  // Auto-strip VERO brand names to prevent IP infringement takedowns
  t = stripVeROFromTitle(t);
  // Strip eBay-banned words
  const BANNED = [/\bauthentic\b/gi, /\bgenuine\b/gi, /\boriginal\b/gi,
    /\bverified\b/gi, /\bcertified\b/gi, /\bauthorized\b/gi];
  for (const re of BANNED) t = t.replace(re, '');
  // Collapse spaces
  t = t.replace(/\s{2,}/g, ' ').trim();
  // Cut at 65 chars at clean word boundary, strip trailing punctuation
  if (t.length > 65) t = t.slice(0, 65).replace(/\s+\S*$/, '').trim();
  t = t.replace(/[,\-–|:]+$/, '').trim();
  return t;
}

// ── Extract price from Amazon HTML ────────────────────────────────────────────
function extractPrice(html) {
  // Try buy-box first to avoid picking up competitor/ad prices
  const buyBox = extractPriceFromBuyBox(html);
  if (buyBox) return buyBox;
  // Fallback: search full HTML but only for very specific patterns
  const pats = [
    /id="priceblock_ourprice"[^>]*>[^$]*\$([\d.]+)/,
    /id="priceblock_dealprice"[^>]*>[^$]*\$([\d.]+)/,
  ];
  for (const p of pats) {
    const m = html.match(p);
    if (m) return parseFloat(m[1]);
  }
  return null;
}

// ── Extract Amazon shipping cost from product page HTML ──────────────────────
// Returns: 0 if FREE shipping, positive number if paid shipping
function extractShippingCost(html) {
  // Priority 1: data-csa-c-delivery-price attribute — most reliable, directly from buy box
  // Amazon sets this even when "FREE delivery" appears elsewhere on the page
  // e.g. data-csa-c-delivery-price="$12.99" means paid shipping
  const csaDeliveryM = html.match(/data-csa-c-delivery-price="\$([\d.]+)"/);
  if (csaDeliveryM) {
    const cost = parseFloat(csaDeliveryM[1]);
    if (cost > 0 && cost < 100) return cost;
    if (cost === 0) return 0; // explicitly free
  }

  // Priority 2: paid shipping patterns in buy box section only
  // Restrict to buy box to avoid matching unrelated "FREE delivery" promos elsewhere
  const buyBox = html.match(/id="(?:deliveryBlockMessage|mir-layout-DELIVERY_BLOCK|shippingMessageInsideBuyBox|deliveryMessage)[\s\S]{0,2000}/)?.[0] || '';

  // Check for paid amount in buy box
  const paidInBuyBox = [
    /\$([\d.]+)\s+delivery/i,
    /\$([\d.]+)\s+shipping/i,
    /"shippingAmount"\s*:\s*\{"amount"\s*:\s*([\d.]+)/,
    /"deliveryPrice"\s*:\s*\{"amount"\s*:\s*([\d.]+)/,
  ];
  for (const p of paidInBuyBox) {
    const m = buyBox.match(p);
    if (m) {
      const cost = parseFloat(m[1]);
      if (cost > 0 && cost < 100) return cost;
    }
  }

  // Priority 3: FREE shipping patterns in buy box
  const freeInBuyBox = buyBox && (
    /FREE\s+(?:delivery|shipping)/i.test(buyBox) ||
    /"isFreeShipping"\s*:\s*true/i.test(buyBox)
  );
  if (freeInBuyBox) return 0;

  // Priority 4: full-page paid shipping (outside buy box)
  const fullPaidPatterns = [
    /\+\s*\$([\d.]+)\s+shipping/i,
    /\$([\d.]+)\s+shipping/i,
    /shipping\s*&amp;\s*handling[^$]*\$([\d.]+)/i,
  ];
  for (const p of fullPaidPatterns) {
    const m = html.match(p);
    if (m) {
      const cost = parseFloat(m[1]);
      if (cost > 0 && cost < 100) return cost;
    }
  }

  // Default: assume free (most Prime/FBA products ship free)
  return 0;
}

// ── Map Amazon dimension key → eBay aspect name ─────────────────────────────────
// Color-like keywords — if style/pattern values contain these, treat dim as Color
const COLOR_KEYWORDS = /^(black|white|gray|grey|silver|gold|beige|brown|blue|red|green|purple|pink|orange|yellow|teal|navy|charcoal|ivory|cream|tan|khaki|olive|multicolor|multi|mixed|color|colour|clear|transparent|natural|oak|walnut|maple|cherry|espresso|chocolate)/i;

function isColorLikeDim(values) {
  if (!values?.length) return false;
  const matches = values.filter(v => COLOR_KEYWORDS.test(String(v).trim()));
  return matches.length >= Math.ceil(values.length * 0.5); // majority look like colors
}

function dimKeyToAspectName(key, values) {
  const MAP = {
    color_name:'Color', size_name:'Size', flavor_name:'Flavor',
    pattern_name:'Style', scent_name:'Scent', material_name:'Material', count_name:'Count',
    item_package_quantity:'Count',
    length_name:'Length', width_name:'Width', configuration_name:'Configuration',
    edition_name:'Edition', finish_name:'Finish', voltage_name:'Voltage',
    style_name:'Style',
  };
  if (MAP[key]) {
    // Auto-upgrade pattern/style to Color if values look like colors (e.g. tire covers)
    if ((key === 'style_name' || key === 'pattern_name') && isColorLikeDim(values)) return 'Color';
    return MAP[key];
  }
  return key.replace('_name','').replace(/^\w/, c => c.toUpperCase());
}

// ── Extract price from buy-box section (more reliable than full HTML) ─────────────
function extractDeliveryDays(html) {
  // Extract estimated delivery days from Amazon product page
  // Returns: number of days until earliest delivery, or null if not found
  if (!html) return null;

  const today = new Date();
  today.setHours(0, 0, 0, 0);

  // Look in delivery message sections
  const section = html.match(/id="delivery-message"[\s\S]{0,800}/)?.[0]
                || html.match(/id="deliveryMessageMirror"[\s\S]{0,800}/)?.[0]
                || html.match(/id="fast-track-message"[\s\S]{0,800}/)?.[0]
                || html.match(/id="mir-layout-DELIVERY_BLOCK[\s\S]{0,800}/)?.[0]
                || html.match(/"deliveryBlock"[\s\S]{0,500}/)?.[0]
                || '';

  const MONTHS = {jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11,
                  january:0,february:1,march:2,april:3,june:5,july:6,august:7,september:8,october:9,november:10,december:11};

  // Pattern: "Arrives by Mon, Apr 14" or "Delivery by April 14" or "Get it by Thu, Apr 10"
  const datePats = [
    /(?:arrives?|delivery|get it|ships?)\s+by\s+(?:\w+,\s+)?(\w+)\s+(\d+)/i,
    /(?:arrives?|delivery|get it)\s+(?:\w+,\s+)?(\w+)\s+(\d+)/i,
    /(\w+)\s+(\d+)\s*[-–]\s*\d+/i,  // "Apr 10-14"
    /(\w+day),\s+(\w+)\s+(\d+)/i,   // "Monday, April 14"
  ];

  for (const pat of datePats) {
    const src = section || html.slice(0, 20000);
    const m = src.match(pat);
    if (m) {
      const monthStr = (m[3] ? m[2] : m[1]).toLowerCase().slice(0, 3);
      const day = parseInt(m[3] || m[2]);
      const monthNum = MONTHS[monthStr];
      if (monthNum !== undefined && day > 0 && day <= 31) {
        const year = today.getMonth() > monthNum ? today.getFullYear() + 1 : today.getFullYear();
        const delivDate = new Date(year, monthNum, day);
        const days = Math.round((delivDate - today) / 86400000);
        if (days >= 0 && days <= 60) return days;
      }
    }
  }

  // Pattern: "within X days" or "X-Y days"
  const relPats = [
    /within\s+(\d+)\s+(?:business\s+)?days?/i,
    /(\d+)\s*-\s*\d+\s+(?:business\s+)?days?/i,
    /in\s+(\d+)\s+(?:business\s+)?days?/i,
  ];
  for (const pat of relPats) {
    const m = (section || html.slice(0, 20000)).match(pat);
    if (m) {
      const d = parseInt(m[1]);
      if (d >= 0 && d <= 60) return d;
    }
  }

  return null; // can't determine delivery days
}

function extractShippingFromPage(html) {
  // Extract per-ASIN shipping cost from Amazon product page
  // Returns: 0 = free shipping, positive number = shipping cost, null = unknown
  if (!html) return null;

  // Check for free shipping signals first
  const freeRx = [/FREE\s+(?:delivery|shipping)/i, /free\s+prime\s+delivery/i,
    /"freeShipping"\s*:\s*true/i, /class="[^"]*FREE[^"]*delivery[^"]*"/i,
    /"shippingMessage"\s*:\s*"[^"]*FREE/i];
  for (const rx of freeRx) { if (rx.test(html)) return 0; }

  const section = html.match(/id="delivery-message"[\s\S]{0,500}/)?.[0]
                || html.match(/id="deliveryMessageMirror"[\s\S]{0,500}/)?.[0]
                || html.match(/id="fast-track-message"[\s\S]{0,500}/)?.[0] || '';

  const shipPats = [/\$([\d,]+\.?\d*)\s+shipping/i, /shipping[:\s]+\$([\d,]+\.?\d*)/i,
    /"shippingCost"\s*:\s*\{"amount"\s*:\s*([\d.]+)/, /\+\s*\$([\d.]+)\s+(?:shipping|delivery)/i];
  for (const rx of shipPats) {
    const m = (section || html).match(rx);
    if (m) { const c = parseFloat(m[1].replace(/,/g,'')); if (c >= 0 && c < 100) return c; }
  }
  return null;
}

function extractIsPrime(html) {
  // Returns true if the product page shows Prime badge/eligibility for this ASIN
  if (!html) return false;
  const primeSignals = [
    /id="primeBadge/i,
    /prime-logo/i,
    /"isPrime"\s*:\s*true/i,
    /class="[^"]*prime[^"]*badge[^"]*"/i,
    /aria-label="[^"]*Amazon\s*Prime[^"]*"/i,
    /FREE\s+Prime\s+delivery/i,
    /FREE\s+delivery[^<]{0,50}Prime/i,
    /"isEligibleForPrime"\s*:\s*true/i,
    /primeIconUrl/i,
    /prime-day/i, // Prime Day badge
    /delivery_promise_tooltip.*prime/i,
    /"programEligibility"[\s\S]{0,200}"eligibleForPrime"\s*:\s*true/i,
  ];
  return primeSignals.some(rx => rx.test(html));
}

function extractPriceFromBuyBox(html) {
  if (!html) return null;
  // ONLY look in the buy-box section — never fall through to full HTML
  const core = html.match(/id="corePrice_feature_div"[\s\S]{0,2000}/)?.[0]
             || html.match(/id="apex_desktop"[\s\S]{0,3000}/)?.[0]
             || html.match(/id="ppd"[\s\S]{0,5000}/)?.[0]
             || '';
  if (!core) return null;

  // Priority 0: apex-pricetopay-accessibility-label — Amazon's newer apex
  // price block puts the actual checkout price as text content of this span:
  //   <span id="apex-pricetopay-accessibility-label" class="aok-offscreen">$799.99</span>
  // It's the most reliable extraction point on the page when present (no
  // whole+fraction joining, no JSON traversal). Try it first across the full
  // HTML, not just `core` — the apex block sometimes lives outside corePrice.
  const apexLabel = html.match(/id="apex-pricetopay-accessibility-label"[^>]*>\s*\$?([\d,]+\.?\d*)\s*</);
  if (apexLabel) {
    const p = parseFloat(apexLabel[1].replace(/,/g, ''));
    if (p > 0 && p < 100000) return p;
  }
  // Same selector with attributes in different order (defensive)
  const apexLabelAlt = html.match(/class="aok-offscreen"[^>]*id="apex-pricetopay-accessibility-label"[^>]*>\s*\$?([\d,]+\.?\d*)\s*</)
                    || html.match(/<span[^>]*aok-offscreen[^>]*pricetopay[^>]*>\s*\$?([\d,]+\.?\d*)\s*</);
  if (apexLabelAlt) {
    const p = parseFloat(apexLabelAlt[1].replace(/,/g, ''));
    if (p > 0 && p < 100000) return p;
  }

  // Priority 1: "priceToPay" — the actual checkout price (discounted if on sale)
  // Pattern: "$35.99 with 5 percent savings" in accessibility label
  const withSavings = core.match(/\$([\d.]+)\s+with\s+\d+\s*percent\s+savings/);
  if (withSavings) {
    const p = parseFloat(withSavings[1]);
    if (p > 0 && p < 10000) return p;
  }

  // Priority 2: apexPriceToPay / centralizedApex — the deal/sale price box
  const apexCore = html.match(/id="apex_desktop"[\s\S]{0,5000}/)?.[0]
                || html.match(/centralizedApex[\s\S]{0,3000}/)?.[0] || '';
  if (apexCore) {
    // Look for the pricetopay span (this is the SALE price, not the strikethrough)
    const apexM = apexCore.match(/class="a-price-whole"[^>]*>\s*(\d[\d,]*)<\/span><span[^>]*class="a-price-fraction"[^>]*>\s*(\d+)/);
    if (apexM) {
      const p = parseFloat(`${apexM[1].replace(/,/g,'')}.${apexM[2]}`);
      if (p > 0 && p < 10000) return p;
    }
  }

  const pats = [
    // Whole + fraction in buy-box span
    /class="a-price-whole"[^>]*>\s*(\d[\d,]*)<\/span><span[^>]*class="a-price-fraction"[^>]*>\s*(\d+)/,
    // priceToPay JSON (most accurate — actual checkout price)
    /"priceToPay"\s*:\s*\{"amount"\s*:\s*([\d.]+)/,
    // priceToPayWithScheme
    /"priceToPayWithScheme"\s*:\s*\{"amount"\s*:\s*([\d.]+)/,
    // buyingPrice
    /"buyingPrice"\s*:\s*\{"amount"\s*:\s*([\d.]+)/,
    // a-offscreen — but prefer priceToPay over a-text-price (strikethrough)
    /class="a-offscreen"[^>]*>\$([\d,]+\.?\d*)/,
    // priceAmount in buy-box JSON
    /"priceAmount"\s*:\s*([\d.]+)/,
    // displayPrice
    /"displayPrice"\s*:\s*"\$([\d,]+\.?\d*)"/,
    // Lambda price
    /"lambdaPrice"\s*:\s*\{"amount"\s*:\s*([\d.]+)/,
  ];
  for (const p of pats) {
    const m = core.match(p);
    if (m) {
      const price = parseFloat(m[2] ? `${m[1].replace(/,/g,'')}.${m[2]}` : m[1].replace(/,/g,''));
      if (price > 0 && price < 10000) return price;
    }
  }
  return null;
}

// ── Extract first hi-res image from an ASIN page ─────────────────────────────────
function extractMainImage(html) {
  // colorImages initial[0] is always the hero product shot
  const block = extractBlock(html, 'colorImages');
  if (block) {
    const m = block.match(/'initial'\s*:\s*\[\s*\{\s*(?:[^{}]*?)"hiRes"\s*:\s*"(https:[^"]+\.jpg)"/);
    if (m) return m[1];
  }
  const m = html.match(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/);
  return m ? m[1] : null;
}

function extractAllImages(html) {
  // Extract ALL hiRes images from colorImages.initial (full per-color gallery)
  const images = [];
  const block = extractBlock(html, 'colorImages');
  const src = block || html;
  const matches = src.matchAll(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/g);
  const seen = new Set();
  for (const m of matches) {
    if (!seen.has(m[1])) { seen.add(m[1]); images.push(m[1]); }
    if (images.length >= 12) break;
  }
  return images;
}

// ── Bracket-counting block extractor ─────────────────────────────────────────
// Properly handles nested objects, strings with apostrophes, escape sequences
function extractBlock(html, searchStr) {
  const idx = html.indexOf(searchStr);
  if (idx === -1) return null;
  let i = idx + searchStr.length;
  while (i < html.length && html[i] !== '{' && html[i] !== '[') i++;
  if (i >= html.length) return null;
  const openChar = html[i], closeChar = openChar === '{' ? '}' : ']';
  let depth = 0, inStr = false, strChar = '', escaped = false;
  const start = i;
  for (; i < Math.min(html.length, start + 500000); i++) {
    const c = html[i];
    if (escaped) { escaped = false; continue; }
    if (c === '\\') { escaped = true; continue; }
    if (inStr) { if (c === strChar) inStr = false; continue; }
    if (c === '"' || c === "'") { inStr = true; strChar = c; continue; }
    if (c === openChar) depth++;
    else if (c === closeChar) { depth--; if (depth === 0) return html.slice(start, i + 1); }
  }
  return null;
}

// ── Extract color→image map from colorImages block ────────────────────────────
// Handles: 'Ivory': [...], "L'Special": [...], 'Brown-Checkered': [...]
// ── Extract color→image from swatch img elements in raw HTML ─────────────────
// Confirmed from live Amazon page: swatch imgs have alt="ColorName" src="...._SS64_.jpg"
function extractSwatchImages(html) {
  const map = {};
  const re = /alt="([^"]{2,60})"[^>]{0,200}src="(https:\/\/m\.media-amazon\.com\/images\/I\/[^"_]+\._SS\d+_\.jpg)"/g;
  const seen = new Set();
  let m;
  while ((m = re.exec(html)) !== null) {
    const name = m[1].trim();
    if (!seen.has(name) && name.length > 1 && !name.toLowerCase().includes('brand')) {
      seen.add(name);
      map[name] = m[2].replace(/\._SS\d+_\.jpg$/, '._SL1500_.jpg');
    }
  }
  console.log('[images] swatch map:', Object.keys(map).length, 'colors');
  return map;
}

// ── Extract colorToAsin + sizeToAsin from "colorToAsin" JSON ─────────────────
// Real format: {"Airy Blue 50\"x60\"": {"asin": "B0XXXXX"}, ...}
function extractColorAsinMaps(html) {
  const colorToAsin = {};
  const sizeToAsin  = {};
  const block = extractBlock(html, '"colorToAsin"');
  if (!block) return { colorToAsin, sizeToAsin };
  let data = {};
  try { data = JSON.parse(block); } catch { return { colorToAsin, sizeToAsin }; }
  for (const [key, val] of Object.entries(data)) {
    const asin = val?.asin || (typeof val === 'string' ? val : null);
    if (!asin || asin.length !== 10) continue;
    // Keys: "Color 50\"x60\"" — literal backslash-quotes. After JSON.parse still have \\"
    const cleaned = key.replace(/\\"/g, '"').replace(/\\'/g, "'");
    // Match: ColorName + space + dimensions like 50"x60" or 60"x80"
    const sizeM = cleaned.match(/^(.+?)\s+(\d+(?:\.\d+)?"[×x]\d+(?:\.\d+)?"?)\s*$/i);
    if (sizeM) {
      const color = sizeM[1].trim();
      const size  = sizeM[2].trim();
      if (!colorToAsin[color]) colorToAsin[color] = asin;
      if (!sizeToAsin[size])   sizeToAsin[size]   = asin;
    } else {
      if (!colorToAsin[cleaned]) colorToAsin[cleaned] = asin;
    }
  }
  console.log('[colorToAsin] colors:', Object.keys(colorToAsin).length, 'sizes:', Object.keys(sizeToAsin).length);
  return { colorToAsin, sizeToAsin };
}

// ── eBay Taxonomy API: get leaf category suggestions ─────────────────────────

// In-process caches — valid for the lifetime of the Node process.
// Policies and locations rarely change — caching saves 3-4 API calls per push
const _policyCache = {}; // token.slice(-8) → { fulfillmentPolicyId, paymentPolicyId, returnPolicyId, ts }
const _locationCache = {}; // 'us'|'cn' → locationKey

async function resolvePolicies(token, supplied) {
  const EBAY_API = getEbayUrls().EBAY_API;
  const auth = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US', 'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US' };
  const p = {
    fulfillmentPolicyId: (String(supplied.fulfillmentPolicyId || '')).trim(),
    paymentPolicyId:     (String(supplied.paymentPolicyId     || '')).trim(),
    returnPolicyId:      (String(supplied.returnPolicyId       || '')).trim(),
  };

  // Return from cache if all 3 supplied IDs are provided (common case — user has set policies in Settings)
  if (p.fulfillmentPolicyId && p.paymentPolicyId && p.returnPolicyId) return p;

  // Check in-process cache (saves 3 API calls on concurrent pushes)
  const _cacheKey = token.slice(-12);
  const _cached = _policyCache[_cacheKey];
  if (_cached && (Date.now() - _cached.ts < 5 * 60 * 1000)) { // 5 min TTL
    if (!p.fulfillmentPolicyId) p.fulfillmentPolicyId = _cached.fulfillmentPolicyId;
    if (!p.paymentPolicyId)     p.paymentPolicyId     = _cached.paymentPolicyId;
    if (!p.returnPolicyId)      p.returnPolicyId      = _cached.returnPolicyId;
    console.log('[policies] served from cache');
    return p;
  }

  if (!p.fulfillmentPolicyId || !p.paymentPolicyId || !p.returnPolicyId) {
    const [fp, pp, rp] = await Promise.all([
      fetch(`${EBAY_API}/sell/account/v1/fulfillment_policy?marketplace_id=EBAY_US`, { headers: auth }).then(r => r.json()).catch(() => ({})),
      fetch(`${EBAY_API}/sell/account/v1/payment_policy?marketplace_id=EBAY_US`,     { headers: auth }).then(r => r.json()).catch(() => ({})),
      fetch(`${EBAY_API}/sell/account/v1/return_policy?marketplace_id=EBAY_US`,      { headers: auth }).then(r => r.json()).catch(() => ({})),
    ]);
    if (!p.fulfillmentPolicyId) p.fulfillmentPolicyId = (fp.fulfillmentPolicies || [])[0]?.fulfillmentPolicyId || '';
    if (!p.paymentPolicyId)     p.paymentPolicyId     = (pp.paymentPolicies     || [])[0]?.paymentPolicyId     || '';
    if (!p.returnPolicyId)      p.returnPolicyId      = (rp.returnPolicies      || []).find(x => x.returnsAccepted)?.returnPolicyId
                                                        || (rp.returnPolicies || [])[0]?.returnPolicyId || '';
    console.log(`[policies] fp=${p.fulfillmentPolicyId?.slice(-8)} pp=${p.paymentPolicyId?.slice(-8)} rp=${p.returnPolicyId?.slice(-8)}`);
    // Write to cache
    _policyCache[_cacheKey] = { ...p, ts: Date.now() };
  }

  if (!p.fulfillmentPolicyId) {
    // Try to create a fulfillment policy with multiple fallback service codes
    // eBay accounts have different supported services depending on registration type
    const serviceAttempts = [
      { shippingServiceCode: 'ShippingMethodStandard', freeShipping: true },
      { shippingServiceCode: 'USPSFirstClass', freeShipping: true },
      { shippingServiceCode: 'USPSPriority', freeShipping: true },
    ];
    for (const svc of serviceAttempts) {
      const r = await fetch(`${EBAY_API}/sell/account/v1/fulfillment_policy`, {
        method: 'POST', headers: auth,
        body: JSON.stringify({
          name: 'DropSync Free Shipping', marketplaceId: 'EBAY_US',
          categoryTypes: [{ name: 'ALL_EXCLUDING_MOTORS_VEHICLES', default: true }],
          handlingTime: { value: 3, unit: 'DAY' },
          shippingOptions: [{ optionType: 'DOMESTIC', costType: 'FLAT_RATE',
            shippingServices: [{
              shippingServiceCode: svc.shippingServiceCode,
              freeShipping: svc.freeShipping,
              shippingCost: { value: '0.00', currency: 'USD' },
              buyerResponsibleForShipping: false,
              sortOrder: 1,
            }],
          }],
        }),
      });
      const d = await r.json();
      if (d.fulfillmentPolicyId) { p.fulfillmentPolicyId = d.fulfillmentPolicyId; break; }
      // If name already exists, fetch it
      if ((d.errors||[]).some(e => e.errorId === 20400)) {
        const existing = await fetch(`${EBAY_API}/sell/account/v1/fulfillment_policy?marketplace_id=EBAY_US`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
        p.fulfillmentPolicyId = (existing.fulfillmentPolicies||[]).find(x=>x.name==='DropSync Free Shipping')?.fulfillmentPolicyId
                              || (existing.fulfillmentPolicies||[])[0]?.fulfillmentPolicyId || '';
        if (p.fulfillmentPolicyId) break;
      }
      console.warn('[policy] create failed with', svc.shippingServiceCode, ':', JSON.stringify(d).slice(0,200));
    }
  }
  if (!p.returnPolicyId) {
    const r = await fetch(`${EBAY_API}/sell/account/v1/return_policy`, {
      method: 'POST', headers: auth,
      body: JSON.stringify({
        name: 'DropSync 30-Day Returns', marketplaceId: 'EBAY_US',
        categoryTypes: [{ name: 'ALL_EXCLUDING_MOTORS_VEHICLES' }],
        returnsAccepted: true, returnPeriod: { value: 30, unit: 'DAY' },
        returnShippingCostPayer: 'BUYER', refundMethod: 'MONEY_BACK',
      }),
    });
    const d = await r.json();
    p.returnPolicyId = d.returnPolicyId || '';
    // If name already exists, fetch it
    if (!p.returnPolicyId && (d.errors||[]).some(e => e.errorId === 20400)) {
      const existing = await fetch(`${EBAY_API}/sell/account/v1/return_policy?marketplace_id=EBAY_US`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
      p.returnPolicyId = (existing.returnPolicies||[]).find(x=>x.name==='DropSync 30-Day Returns')?.returnPolicyId
                       || (existing.returnPolicies||[])[0]?.returnPolicyId || '';
    }
  }

  if (!p.fulfillmentPolicyId) throw new Error(
    'No shipping policy available. ' +
    'Go to eBay Seller Hub → Account → Business Policies → Create a Shipping Policy, ' +
    'then select it in DropSync Settings.'
  );
  if (!p.returnPolicyId) throw new Error(
    'No return policy available. ' +
    'Go to eBay Seller Hub → Account → Business Policies → Create a Return Policy, ' +
    'then select it in DropSync Settings.'
  );
  console.log(`[policies] resolved fp=${p.fulfillmentPolicyId} pp=${p.paymentPolicyId} rp=${p.returnPolicyId}`);
  return p;
}

// ── Ensure merchant location exists ──────────────────────────────────────────
async function ensureLocation(auth, isAliExpress = false) {
  const _locKey = isAliExpress ? 'cn' : 'us';
  if (_locationCache[_locKey]) return _locationCache[_locKey];
  const EBAY_API = getEbayUrls().EBAY_API;
  const r = await fetch(`${EBAY_API}/sell/inventory/v1/location`, { headers: auth }).then(r => r.json()).catch(() => ({}));
  const locations = r.locations || [];
  // For AliExpress: find or create a China warehouse location
  if (isAliExpress) {
    const cnLoc = locations.find(l => l.merchantLocationKey === 'ChinaWarehouse' || l.location?.address?.country === 'CN');
    if (cnLoc) { const _ck = cnLoc.merchantLocationKey; _locationCache[_locKey] = _ck; console.log(`[location] found China: ${_ck}`); return _ck; }
    const cnKey = 'ChinaWarehouse';
    const cr = await fetch(`${EBAY_API}/sell/inventory/v1/location/${cnKey}`, {
      method: 'POST', headers: auth,
      body: JSON.stringify({
        location: { address: { addressLine1: 'Guangdong Province', city: 'Shenzhen', country: 'CN', postalCode: '518000' } },
        locationTypes: ['WAREHOUSE'],
        name: 'China Warehouse',
        merchantLocationStatus: 'ENABLED',
      }),
    });
    const cnD = await cr.json().catch(() => ({}));
    console.log(`[location] China create: ${cr.status}`);
    if (cr.ok || cr.status === 204 || cr.status === 409) { _locationCache[_locKey] = cnKey; return cnKey; }
    // Fallback to US warehouse if China creation fails
  }
  if (locations.length) {
    const key = locations[0].merchantLocationKey;
    console.log(`[location] found existing: ${key}`);
    _locationCache[_locKey] = key;
    return key;
  }
  const key = 'MainWarehouse';
  const cr = await fetch(`${EBAY_API}/sell/inventory/v1/location/${key}`, {
    method: 'POST', headers: auth,
    body: JSON.stringify({
      location: { address: { addressLine1: '123 Main St', city: 'San Jose', stateOrProvince: 'CA', postalCode: '95125', country: 'US' } },
      locationTypes: ['WAREHOUSE'],
      name: key,
      merchantLocationStatus: 'ENABLED',
    }),
  });
  const cd = await cr.json().catch(() => ({}));
  console.log(`[location] create result: ${cr.status} ${JSON.stringify(cd).slice(0,200)}`);
  // Wait for eBay to propagate the new location before using it
  await sleep(3000);
  _locationCache[_locKey] = key;
  return key;
}

// ── Build offer payload ───────────────────────────────────────────────────────
function buildOffer(sku, price, categoryId, policies, locationKey) {
  return {
    sku, marketplaceId: 'EBAY_US', format: 'FIXED_PRICE', listingDuration: 'GTC',
    pricingSummary: { price: { value: String(parseFloat(price || 0).toFixed(2)), currency: 'USD' } },
    categoryId: String(categoryId),
    merchantLocationKey: locationKey,
    listingPolicies: {
      fulfillmentPolicyId: policies.fulfillmentPolicyId,
      paymentPolicyId:     policies.paymentPolicyId,
      returnPolicyId:      policies.returnPolicyId,
    },
    tax: { applyTax: true },
  };
}

// ── Shared Amazon scraper — called by both scrape and revise actions ───────────
// Returns the full product object (same shape as scrape action), or null on failure.
// ── Extract images for the currently selected variant from colorImages.initial ──
function extractVariantImages(h) {
  const imgSet = [];
  const seen = new Set();

  function extractArr(s, start) {
    let depth = 0, i = start, inStr = false, esc = false;
    for (; i < s.length && i < start + 200000; i++) {
      const c = s[i];
      if (esc) { esc = false; continue; }
      if (c === '\\') { esc = true; continue; }
      if (inStr) { if (c === '"') inStr = false; continue; }
      if (c === '"') { inStr = true; continue; }
      if (c === '[') depth++;
      else if (c === ']') { depth--; if (depth === 0) return s.slice(start, i+1); }
    }
    return null;
  }

  // 1. colorImages 'initial' array — images for the currently selected variant only
  const ciIdx = h.indexOf("'colorImages'");
  if (ciIdx > -1) {
    const initIdx = h.indexOf("'initial'", ciIdx);
    if (initIdx > -1 && initIdx < ciIdx + 1000) {
      const arrStart = h.indexOf('[', initIdx);
      if (arrStart > -1 && arrStart < initIdx + 100) {
        const initArr = extractArr(h, arrStart);
        if (initArr) {
          for (const m of initArr.matchAll(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/g)) {
            if (!seen.has(m[1])) { seen.add(m[1]); imgSet.push(m[1]); }
            if (imgSet.length >= 12) break;
          }
        }
      }
    }
  }

  // 2. imageGalleryData block if needed
  if (imgSet.length < 6) {
    const igIdx = h.indexOf('"imageGalleryData"');
    if (igIdx > -1) {
      const arrStart2 = h.indexOf('[', igIdx);
      if (arrStart2 > -1 && arrStart2 < igIdx + 50) {
        const igArr = extractArr(h, arrStart2);
        if (igArr) {
          for (const m of igArr.matchAll(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/g)) {
            if (!seen.has(m[1])) { seen.add(m[1]); imgSet.push(m[1]); }
            if (imgSet.length >= 12) break;
          }
        }
      }
    }
  }

  // 3. Fallback: landingImage then first hiRes anywhere
  if (imgSet.length === 0) {
    const liM = h.match(/id="landingImage"[^>]*data-old-hires="(https:[^"]+)"/);
    if (liM) imgSet.push(liM[1]);
    if (imgSet.length === 0) {
      const m = h.match(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/);
      if (m) imgSet.push(m[1]);
    }
  }

  console.log('[images] extractVariantImages:', imgSet.length, 'images');
  return imgSet;
}

async function scrapeAmazonProduct(inputUrl, preloadedHtml = null, clientAsinData = null, primeOnly = false) {
  let url = (inputUrl || '').trim();
  if (!url) return null;

  // Normalize to clean dp/ASIN URL
  const asinM = url.match(/\/(?:dp|gp\/product)\/([A-Z0-9]{10})/);
  const asinP = url.match(/[?&]asin=([A-Z0-9]{10})/i);
  const asin  = asinM?.[1] || asinP?.[1];
  if (asin) url = `https://www.amazon.com/dp/${asin}?th=1`;

  const ua = randUA();
  // Use pre-fetched HTML if provided by browser (bypasses datacenter-IP blocking)
  let html = preloadedHtml || null;
  if (!html) {
    html = await fetchPage(url, ua);
    if (!html && asin) { await sleep(1500); html = await fetchPage(`https://www.amazon.com/dp/${asin}?psc=1`, ua); }
    if (!html && asin) { await sleep(2000); html = await fetchPage(`https://www.amazon.com/product/dp/${asin}`, ua); }
  } else {
    console.log('[scrapeAmazonProduct] using client-provided HTML — skipping server-side fetch');
  }
  if (!html) return null;

  // ── Parent-ASIN normalization ──────────────────────────────────────────────
  // If the URL we just fetched is a child variant, re-fetch the parent ASIN
  // page instead. Parent pages give:
  //   - a more stable URL (parents almost never go OOS / get suppressed)
  //   - Amazon's "preferred" default variant (more likely to have a real buy-box)
  //   - the same complete dimensionToAsinMap / variationValues / sortedDimValuesForAllDims
  // Cost: 1 extra fetch on import. Skipped if browser pre-fetched HTML
  // (clientAsinData path) since we trust what the browser sent.
  if (!preloadedHtml && asin) {
    const _parentMatch = html.match(/"parentAsin"\s*:\s*"([A-Z0-9]{10})"/);
    const _parentAsin  = _parentMatch?.[1];
    if (_parentAsin && _parentAsin !== asin) {
      console.log(`[scrapeAmazonProduct] child ASIN ${asin} → re-fetching parent ${_parentAsin} for cleaner data`);
      await sleep(800); // brief pause so we don't hammer Amazon back-to-back
      const _parentHtml = await fetchPage(`https://www.amazon.com/dp/${_parentAsin}?th=1`, randUA());
      if (_parentHtml) {
        html = _parentHtml;
        url  = `https://www.amazon.com/dp/${_parentAsin}?th=1`;
        // Note: we keep the original `asin` variable as the imported child's ASIN
        // so downstream code that references `asin` (logging, dedupe keys) still works.
        // The parent ASIN gets stored on product.parentAsin a few lines below.
        console.log(`[scrapeAmazonProduct] parent fetch ok — using parent HTML for variation matrix`);
      } else {
        console.log(`[scrapeAmazonProduct] parent fetch failed — falling back to child HTML`);
      }
    }
  }

  const product = {
    url, source: 'amazon', asin: asin || '',
    title: '', price: 0, images: [],
    description: '', aspects: {}, breadcrumbs: [],
    variations: [], variationImages: {}, hasVariations: false,
    inStock: true, quantity: 1,
    bullets: [], descriptionPara: '',
    comboAsin: {}, sizePrices: {}, comboPrices: {},
  };

  // Title — handles direct text, span wrapper, and whitespace/newlines
  const _titleBlock = html.match(/id="productTitle"[^>]*>([\s\S]{0,500}?)(?=<\/h1>|<\/span>\s*<\/h1>)/)?.[1] || '';
  const _titleText = _titleBlock.replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&#(\d+);/g,(_,n)=>String.fromCharCode(n)).replace(/\s+/g,' ').trim();
  if (_titleText.length >= 5) product.title = _titleText; // cleaned below after varVals is parsed
  // Fallback: meta og:title
  if (!product.title) {
    const _ogM = html.match(/property="og:title"\s+content="([^"]{5,})"/);
    if (_ogM) product.title = _ogM[1].replace(/\s+/g,' ').trim();
  }
  // Fallback: <title> tag (Amazon format: "Product name : Category")
  if (!product.title) {
    const _titleTagM = html.match(/<title>([^<]{5,}?)\s*(?::|Amazon\.com)/);
    if (_titleTagM) product.title = _titleTagM[1].replace(/\s+/g,' ').trim();
  }

  // Breadcrumbs
  const bcRaw = [...html.matchAll(/class="a-link-normal"[^>]*>\s*([^<]{2,40})\s*<\/a>/g)];
  product.breadcrumbs = bcRaw.slice(0, 6).map(m => m[1].trim()).filter(s => s.length > 1 && !/^\d+$/.test(s));

  // price will be set after combo prices are built (cheapest in-stock variant)
  // For single-variant products, use buy-box price
  product.price = 0; // overridden below for single-variant or set from comboPrices
  product._basePrice = extractPriceFromBuyBox(html) || extractPrice(html) || 0;
  product.isPrime    = extractIsPrime(html); // true = Prime-eligible, false = non-Prime

  // Extract parentAsin — used for duplicate detection across variant ASINs
  const parentAsinM2 = html.match(/"parentAsin"\s*:\s*"([A-Z0-9]{10})"/);
  if (parentAsinM2?.[1]) product.parentAsin = parentAsinM2[1];
  // Stock — buy-box only to avoid false OOS from ads/recommendations
  const _bbMain1 = (html.match(/id="availability"[\s\S]{0,3000}/)?.[0]||'')
                 + (html.match(/id="addToCart_feature_div"[\s\S]{0,1000}/)?.[0]||'');
  if (html.toLowerCase().includes('see all available options') && !html.includes('id="add-to-cart-button"')) {
    product.inStock = false;  // "See all available options" = no direct buy = OOS
  } else if (_bbMain1.length > 30) {
    product.inStock = !isAmazonOOS(_bbMain1);
  } else {
    // No buy-box found — check for strong signals
    product.inStock = html.includes('id="add-to-cart-button"')
                   || (!html.includes('id="outOfStock"') && !html.includes('Currently unavailable'));
  }

  // Images — two separate pools:
  // 1. product.images = the current variant's clean gallery (for eBay main listing)
  // 2. allHiRes = ALL unique hiRes from the full page (for per-color image fallback)
  product.images = extractVariantImages(html);
  // Build full hiRes pool for colorImgMap fallback (deduplicated, capped at 50)
  const allHiRes = [...new Set(
    [...html.matchAll(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/g)]
      .map(m => m[1])
  )].slice(0, 50);

  // Bullets
  // Garbage patterns — bullet text matching any of these is discarded
  const BULLET_GARBAGE = [
    /image\s*(unavailable|not\s*available)/i,
    /select\s*your\s*(preferred|size|color)/i,
    /click\s*to\s*open\s*expanded/i,
    /your\s*orders/i, /drop\s*off/i,
    /clothing,\s*shoes/i, /\btops,\s*tees/i,
    /›|&gt;/,                      // breadcrumb arrows
    /\$[\d.]+/,                    // prices
    /^\s*(\d+\.?\d*%?\s*){1,3}$/, // just numbers/percentages
    /ships\s*from/i, /add\s*to\s*cart/i,
    /free\s*(shipping|returns|delivery)/i,
    /sign\s*in/i, /log\s*in/i,
    /see\s*more/i, /read\s*more/i,
    /customers\s*also/i, /frequently\s*bought/i,
  ];
  const isBulletGarbage = txt =>
    txt.length < 15 || txt.length > 600 ||
    BULLET_GARBAGE.some(p => p.test(txt));

  const extractBullets = (h) => {
    const results = [];
    const sectionPatterns = [
      /id="productFactsDesktopExpander"[^>]*>([\s\S]{100,8000}?)<\/div>\s*<\/div>\s*<\/div>/,
      /id="featurebullets_feature_div"[^>]*>([\s\S]{100,6000}?)<\/div>\s*<\/div>/,
      /id="feature-bullets"[^>]*>([\s\S]{100,6000}?)<\/div>\s*<\/div>/,
    ];
    let sectionHtml = '';
    for (const pat of sectionPatterns) { const m = h.match(pat); if (m) { sectionHtml = m[1]; break; } }
    if (sectionHtml) {
      for (const m of [...sectionHtml.matchAll(/<li[^>]*>[\s\S]*?<span[^>]*class="[^"]*a-list-item[^"]*"[^>]*>([\s\S]{15,600}?)<\/span>/g)]) {
        const text = m[1].replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&#\d+;/g,'').replace(/\s+/g,' ').trim();
        if (!isBulletGarbage(text)) results.push(text);
      }
    }
    if (!results.length) {
      for (const m of [...h.matchAll(/<li[^>]*>\s*<span[^>]*a-list-item[^"]*"[^>]*>([\s\S]{20,600}?)<\/span>/g)]) {
        const text = m[1].replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').replace(/\s+/g,' ').trim();
        if (!isBulletGarbage(text)) { results.push(text); }
        if (results.length >= 8) break;
      }
    }
    return results.slice(0, 8);
  };
  product.bullets = extractBullets(html);

  const descParaM = html.match(/id="productDescription"[^>]*>[\s\S]{0,200}<p[^>]*>([\s\S]{30,1000}?)<\/p>/);
  product.descriptionPara = descParaM ? descParaM[1].replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').trim() : '';

  // Build eBay description
  const buildEbayDesc = (title, buls, para, aspects) => {
    const bulletHtml = buls.length ? '<ul>' + buls.map(b => `<li>${b.replace(/</g,'&lt;').replace(/>/g,'&gt;')}</li>`).join('') + '</ul>' : '';
    const specRows = Object.entries(aspects||{}).filter(([k,v])=>!['ASIN','UPC','Color','Size','Brand Name','Brand'].includes(k)&&v[0]&&v[0].length<80).slice(0,10).map(([k,v])=>`<tr><td><b>${k}</b></td><td>${v[0]}</td></tr>`).join('');
    const specsTable = specRows ? `<br/><table border="0" cellpadding="4" cellspacing="0" width="100%"><tbody>${specRows}</tbody></table>` : '';
    return [`<h2>${title}</h2>`,bulletHtml,para?`<p>${para}</p>`:'',specsTable,'<br/><p style="font-size:11px;color:#888">Ships from US. Item is new. Please message us with any questions before purchasing.</p>'].filter(Boolean).join('\n');
  };
  product.description = buildEbayDesc(product.title, product.bullets, product.descriptionPara, product.aspects) || product.title;

  // Item specifics
  for (const [,k,v] of [...html.matchAll(/<th[^>]*class="[^"]*prodDetSectionEntry[^"]*"[^>]*>([^<]+)<\/th>\s*<td[^>]*>([^<]+)<\/td>/gi)]) {
    const key = k.trim(), val = v.trim().replace(/\s+/g,' ');
    if (key && val && val.length < 120 && !val.includes('›')) product.aspects[key] = [val];
  }
  for (const [,k,v] of [...html.matchAll(/(Brand|Material|Color|Size|Style|Weight|Dimensions?)\s*:\s*([^\n<]{2,60})/g)]) {
    if (!product.aspects[k]) product.aspects[k] = [v.trim()];
  }

  // Variations
  // ── Variation parsing — handles ALL Amazon listing types ─────────────────────
  const swatchImgMap = extractSwatchImages(html);

  // Detect ALL variation dimensions from variationValues (not just color+size)
  let varVals = null;
  const vvM = html.match(/"variationValues"\s*:\s*(\{(?:[^{}]|\{[^{}]*\})*\})/);
  if (vvM) try { varVals = JSON.parse(vvM[1]); } catch {}

  // Build dimension list: any *_name key with values
  // Clean title now that varVals is available — strip color/size/dims
  if (product.title) {
    const _allDimVals = Object.values(varVals||{}).flat().filter(Boolean);
    const _cleanedTitle = cleanAmazonTitle(product.title, _allDimVals);
    if (_cleanedTitle && _cleanedTitle.length >= 5) product.title = _cleanedTitle;
  }

    const allDims = [];
  if (varVals) {
    for (const [key, vals] of Object.entries(varVals)) {
      if (Array.isArray(vals) && vals.length) allDims.push({ key, name: dimKeyToAspectName(key, vals), values: vals });
    }
  }

  // If variationValues missing from server HTML (truncated response), bootstrap from sortedDimValuesForAllDims
  // sortedDimValues has displayValue + indexInDimList + defaultAsin — enough to recover dims AND build comboAsin
  if (!allDims.length) {
    const _sddIdx0 = html.indexOf('"sortedDimValuesForAllDims"');
    console.log(`[scraper] bootstrap: sddIdx=${_sddIdx0} htmlLen=${html.length} allDims=${allDims.length}`);
    if (_sddIdx0 > -1) {
      const _dimKeys0 = ['color_name','size_name','style_name','flavor_name','scent_name','pattern_name','configuration_name'];
      for (const _dk0 of _dimKeys0) {
        const _di0 = html.indexOf(`"${_dk0}"`, _sddIdx0);
        if (_di0 < 0) { console.log(`[scraper] bootstrap: ${_dk0} not found`); continue; }
        if (_di0 > _sddIdx0 + 500000) { console.log(`[scraper] bootstrap: ${_dk0} too far (${_di0 - _sddIdx0} chars)`); continue; }
        const _as0 = html.indexOf('[', _di0);
        if (_as0 < 0) continue;
        let _i0 = _as0, _dep0 = 0;
        for (; _i0 < html.length && _i0 < _as0 + 2000000; _i0++) {
          if (html[_i0]==='[') _dep0++; else if (html[_i0]===']') { _dep0--; if (!_dep0) break; }
        }
        try {
          const _arr0 = JSON.parse(html.slice(_as0, _i0 + 1));
          const _vals0 = [];
          const _asinByIdx0 = {}; // idx → defaultAsin
          for (const _e0 of _arr0) {
            const _dv0 = _e0.dimensionValueDisplayText || _e0.displayValue || _e0.value || '';
            const _idx0 = _e0.indexInDimList ?? _vals0.length;
            if (_dv0 && !_vals0.includes(_dv0)) _vals0.push(_dv0);
            if (_e0.defaultAsin) _asinByIdx0[_idx0] = _e0.defaultAsin;
          }
          console.log(`[scraper] bootstrap ${_dk0}: arr=${_arr0.length} vals=${_vals0.length} asins=${Object.keys(_asinByIdx0).length} sampleEntry=${JSON.stringify(_arr0[0]).slice(0,80)}`);
          if (_vals0.length > 0) {
            allDims.push({ key: _dk0, name: dimKeyToAspectName(_dk0, _vals0), values: _vals0, _asinByIdx: _asinByIdx0 });
            if (!varVals) varVals = {};
            varVals[_dk0] = _vals0;
            console.log(`[scraper] bootstrapped dim from sortedDimValues: ${_dk0} (${_vals0.length} vals, ${Object.keys(_asinByIdx0).length} ASINs) sample=${_vals0.slice(0,3)}`);
          }
        } catch(_e0) {}
      }
    }
  }

  // Also check sortedDimValuesForAllDims — Amazon sometimes omits dims from variationValues
  // when the dim has only 1 value on the current page but multiple exist across ASINs.
  // Pull dim names + value counts from sortedDimValues as a supplement.
  try {
    const sddIdx2 = html.indexOf('"sortedDimValuesForAllDims"');
    if (sddIdx2 > -1) {
      const dimCandidates = ['style_name','size_name','color_name','flavor_name','scent_name','pattern_name','configuration_name'];
      for (const dimKey of dimCandidates) {
        if (varVals?.[dimKey]) continue; // already in varVals
        const dIdx2 = html.indexOf(`"${dimKey}"`, sddIdx2);
        if (dIdx2 < 0 || dIdx2 > sddIdx2 + 500000) continue;
        const arrStart2 = html.indexOf('[', dIdx2);
        if (arrStart2 < 0) continue;
        let i2 = arrStart2, depth2 = 0, end2 = -1;
        for (; i2 < html.length && i2 < arrStart2 + 1000000; i2++) {
          if (html[i2]==='[') depth2++;
          else if (html[i2]===']') { depth2--; if (!depth2) { end2=i2; break; } }
        }
        if (end2 < 0) continue;
        try {
          const dimArr2 = JSON.parse(html.slice(arrStart2, end2+1));
          const vals2 = dimArr2.map(e => e.dimensionValueDisplayText || e.displayValue || e.value || '').filter(Boolean);
          if (vals2.length > 1) {
            // This dim has multiple values but wasn't in variationValues — add it
            allDims.push({ key: dimKey, name: dimKeyToAspectName(dimKey, vals2), values: vals2 });
            if (!varVals) varVals = {};
            varVals[dimKey] = vals2;
            console.log(`[scraper] found extra dim from sortedDimValues: ${dimKey} (${vals2.length} values)`);
          }
        } catch(e2) {}
      }
    }
  } catch(e) {}

  const hasVar = allDims.length > 0;
  product.hasVariations = hasVar;

  if (hasVar) {
    // Skip single-value dims ONLY when there are other multi-value dims to use.
    // If ALL dims have 1 value, keep them all — dtaMap may have multiple entries
    // that vary by a dimension Amazon reported as single-value (e.g. style_name with 1 entry
    // in variationValues but multiple ASINs in dtaMap).
    const meaningfulDims = allDims.filter(d => d.values.length > 1);
    const effectiveDims = meaningfulDims.length > 0 ? meaningfulDims : allDims;

    // Log skipped single-value dimensions for debugging
    const skippedDims = allDims.filter(d => d.values.length === 1 && meaningfulDims.length > 0);
    if (skippedDims.length) console.log(`[scraper] skipping single-value dims: ${skippedDims.map(d=>d.name+'='+d.values[0]).join(', ')}`);
    if (meaningfulDims.length === 0) console.log(`[scraper] all dims single-value — keeping all, will detect variation from dtaMap`);

    // ── Smart dim role detection ────────────────────────────────────────────────
    // Detects which dims are price-drivers (pack counts) vs real eBay dimensions,
    // then picks primary/secondary based on what buyers filter by first.
    //
    // Price-driver = size_name whose values are pack quantities ("1 Pack", "2 Pack"...)
    // → excluded from eBay dims, price inherited via size-price fallback
    // Real size = "Small/Medium", "10'x14'", "28W×30L" → keep as eBay dim
    // Pack-size dim detection: catches "1 Pack"/"2 Pack" in size_name OR plain integers in number_of_items
    const _isPackSizeDim = d => {
      if (/^number.of.items$/i.test(d.name)) return true; // number_of_items is always a quantity tier
      if (/^size/i.test(d.name) && d.values.some(v => /^\d+\s*-?\s*pack/i.test(v))) return true;
      // size_name where ALL values are plain integers (e.g. ["1","2","24"]) = pack count
      if (/^size/i.test(d.name) && d.values.length > 0 && d.values.every(v => /^\d+$/.test(String(v).trim()))) return true;
      // size_name where values have VARYING "(Pack of N)" counts = pack-count dimension
      // e.g. ["3 Ounce (Pack of 12)", "3 Ounce (Pack of 24)"] — different qty tiers → price driver
      // BUT: ["42\"W x 63\"L (Pack of 2)", "55\"W x 84\"L (Pack of 2)"] — ALL same (Pack of 2)
      //      → just means "2-panel set", NOT a quantity dimension → don't treat as pack-size
      if (/^size/i.test(d.name) && d.values.length > 0 && d.values.every(v => /\(Pack of \d+\)/i.test(String(v)))) {
        const _packNums = d.values.map(v => String(v).match(/\(Pack of (\d+)\)/i)?.[1]).filter(Boolean);
        const _uniquePacks = new Set(_packNums);
        // Only a price-driving pack dim if values have DIFFERENT pack counts (1,10,24 etc)
        if (_uniquePacks.size > 1) return true;
        // All same pack count = cosmetic suffix, not a quantity tier
      }
      return false;
    };
    const _packSizeDim   = effectiveDims.find(_isPackSizeDim);
    // Non-pack dims only — these are the real eBay variation dimensions
    const _ebayDims = effectiveDims.filter(d => !_isPackSizeDim(d));

    // Role priority (what buyers filter by first → primary):
    // fit_type > style > color > pattern > first
    // Secondary = next most important after primary
    const _fitDim    = _ebayDims.find(d => {
      if (!/^fit.?type|^gender/i.test(d.name)) return false;
      // Only treat as "fit priority" when values are cut/gender words (Regular, Slim, Men's)
      // NOT when values are measurements like "27" Inseam", "29 inch" etc
      const hasMeasurement = d.values.some(v => /\d+["']?\s*(inch|inseam|cm|mm|["'])/i.test(String(v)) || /^\d+\s*["']/.test(String(v).trim()));
      return !hasMeasurement;
    });
    const _styleDim  = _ebayDims.find(d => /^style/i.test(d.name));
    const _colorDim  = _ebayDims.find(d => /color|colour/i.test(d.name));
    const _patternDim= _ebayDims.find(d => /pattern|print|design/i.test(d.name));
    const _sizeDim   = _ebayDims.find(d => /^size/i.test(d.name)); // real size (not pack)

    let primaryDim, secondaryDim;
    if (_fitDim) {
      // fit_type / gender (Men/Women/Youth) → most fundamental filter
      primaryDim   = _fitDim;
      secondaryDim = _colorDim || _patternDim || _styleDim || _ebayDims.find(d => d !== primaryDim) || null;
      console.log(`[scraper] dim-role: fit=primary ${secondaryDim?.name||'none'}=secondary${_packSizeDim?' pack=price-driver':''}`);
    } else if (_styleDim && (_colorDim || _packSizeDim) && !_sizeDim) {
      // style + color (no real size) → style=primary, color=secondary, pack=price-driver
      primaryDim   = _styleDim;
      secondaryDim = _colorDim || _ebayDims.find(d => d !== primaryDim) || null;
      console.log(`[scraper] dim-role: style=primary ${secondaryDim?.name||'none'}=secondary${_packSizeDim?' pack=price-driver':''}`);
    } else if (_sizeDim && (_styleDim || _patternDim) && !_colorDim) {
      // size + thickness/firmness/style (mattress, appliance, etc.):
      // size is ALWAYS primary — buyers choose bed/clothing size first, price varies hugely by size
      // style or pattern = secondary, the other becomes compound-appended extra
      primaryDim   = _sizeDim;
      secondaryDim = _styleDim || _patternDim || _ebayDims.find(d => d !== primaryDim) || null;
      console.log(`[scraper] dim-role: size=primary ${secondaryDim?.name||'none'}=secondary (size×style/pattern listing)`);
    } else if (_patternDim && _styleDim && !_colorDim) {
      // pattern × style (e.g. iOttie mount: dashboard pattern × mount style)
      // pattern = primary (visual), style = secondary (variant type)
      primaryDim   = _patternDim;
      secondaryDim = _styleDim;
      console.log(`[scraper] dim-role: pattern=primary style=secondary`);
    } else {
      // Standard: color > pattern > style > first; size always secondary
      primaryDim   = _colorDim || _patternDim || _styleDim || _ebayDims[0] || effectiveDims[0];
      secondaryDim = _ebayDims.find(d => d !== primaryDim) || null;
      if (_packSizeDim) console.log(`[scraper] dim-role: ${primaryDim?.name}=primary pack=price-driver`);
    }
    primaryDim   = primaryDim   || effectiveDims[0];
    secondaryDim = secondaryDim || effectiveDims.find(d => d !== primaryDim) || null;
    // When number_of_items dim exists, Amazon embeds pack count in color names:
    // "Black (1-pack)", "Black (24-pack)" → strip to just "Black"
    // This deduplicates colors and prevents phantom eBay variants
    if (varVals?.number_of_items && varVals?.color_name) {
      varVals.color_name = [...new Set(
        varVals.color_name.map(c => String(c).replace(/\s*\(\d+-?\s*pack\)/gi, '').trim())
      )];
      console.log(`[scraper] stripped pack suffixes from color_name: ${varVals.color_name.length} clean colors`);
    }

    // Strip invisible/zero-width characters from dimension values
    // Amazon occasionally embeds ZWNJ (U+200C), ZWJ (U+200D), ZWSP (U+200B), BOM (U+FEFF)
    // in product data — these corrupt SKU generation and eBay aspect values
    const _stripInvisible = s => typeof s === 'string'
      ? s.replace(/[​‌‍﻿­⁠]/g, '').trim()
      : s;
    if (primaryDim?.values) primaryDim.values = primaryDim.values.map(_stripInvisible);
    if (secondaryDim?.values) secondaryDim.values = secondaryDim.values.map(_stripInvisible);
    // Also strip from varVals and comboAsin keys
    if (varVals) {
      for (const [k, vals] of Object.entries(varVals)) {
        if (Array.isArray(vals)) varVals[k] = vals.map(_stripInvisible);
      }
    }
    const primaryVals   = primaryDim.values;
    const secondaryVals = secondaryDim?.values || [];

    // Parse dimension order from page
    let dimOrder = null;
    const dimM = html.match(/"dimensions"\s*:\s*(\[[^\]]{0,400}\])/s);
    if (dimM) try { dimOrder = JSON.parse(dimM[1]); } catch {}
    // FIX: dimOrder must never be null — null.length crashes silently via .catch(()=>null)
    // The note said "leave as []" but code set null. Range detection works fine with [].
    if (!dimOrder || !Array.isArray(dimOrder)) dimOrder = [];

    // Build comboAsin map from dimensionToAsinMap
    const dtaBlock = extractBlock(html, '"dimensionToAsinMap"');
    let dtaMap = {};
    try { dtaMap = JSON.parse(dtaBlock); } catch {}

    // ── Extract OOS from sortedDimValuesForAllDims ───────────────────────────────
    // Works for ALL listing types:
    //   COLOR-KEYED (Type A): 1 ASIN per color, shared across sizes
    //     → UNAVAILABLE color entry has defaultAsin → marks all sizes OOS for that color
    //   SIZE-KEYED (Type B): 1 ASIN per size, shared across colors
    //     → UNAVAILABLE size entry has defaultAsin → marks all colors OOS for that size
    //   FULLY-MULTI-ASIN (Type C): every combo has its own ASIN
    //     → UNAVAILABLE entry has defaultAsin for that specific combo
    //   ANY TYPE: UNAVAILABLE displayValue name → matched against prim/sec dim values by name
    const unavailableAsins  = new Set(); // ASIN-based: most reliable, type-agnostic
    const unavailPrimNames  = new Set(); // primary dim value names that are UNAVAILABLE
    const unavailSecNames   = new Set(); // secondary dim value names that are UNAVAILABLE
    try {
      const sddIdx = html.indexOf('"sortedDimValuesForAllDims"');
      if (sddIdx > -1) {
        const dimKeys = Object.keys(varVals || {});
        const allDimKeys = dimKeys.length ? dimKeys : ['color_name', 'size_name', 'style_name', 'flavor_name', 'scent_name'];
        // Figure out which dimKey corresponds to which dimension (primary vs secondary)
        // Primary dim = first in dimOrder (usually color/style), secondary = second (usually size)
        const primDimKey = dimOrder?.[0] || allDimKeys[0];
        const secDimKey  = dimOrder?.[1] || allDimKeys[1];
        for (const dimKey of allDimKeys) {
          const dIdx = html.indexOf(`"${dimKey}"`, sddIdx);
          if (dIdx < 0) continue;
          let i = html.indexOf('[', dIdx), d = 0, st = i;
          if (i < 0 || i > dIdx + 500) continue;
          for (; i < html.length && i < st + 500000; i++) {
            if (html[i]==='[') d++; else if (html[i]===']') { d--; if (!d) break; }
          }
          const dimArr = JSON.parse(html.slice(st, i+1));
          let unavailCount = 0;
          for (const e of dimArr) {
            // Mark OOS for UNAVAILABLE state OR when displayText says "See all available options"
            const _isSeeAll = typeof e.dimensionValueDisplayText === 'string' &&
              /see all available/i.test(e.dimensionValueDisplayText);
            // UNAVAILABLE = "Currently unavailable", AVAILABLE_NOT_PURCHASABLE = "Temporarily out of stock"
            const _isUnavail = e.dimensionValueState === 'UNAVAILABLE' || e.dimensionValueState === 'AVAILABLE_NOT_PURCHASABLE';
            if (!_isUnavail && !_isSeeAll) continue;
            // 1. ASIN-based (highest confidence — directly identifies the unavailable product)
            if (e.defaultAsin) unavailableAsins.add(e.defaultAsin);
            // 2. Name-based (fallback for when defaultAsin is missing)
            //    displayValue is the human-readable name (e.g. "Red", "X-Large")
            const valName = e.displayValue || e.value || e.asin; // Amazon uses displayValue
            if (valName && typeof valName === 'string') {
              if (dimKey === primDimKey || primaryVals.some(pv => pv.toLowerCase() === valName.toLowerCase())) {
                unavailPrimNames.add(valName.toLowerCase());
              } else if (dimKey === secDimKey || secondaryVals.some(sv => sv.toLowerCase() === valName.toLowerCase())) {
                unavailSecNames.add(valName.toLowerCase());
              } else {
                // Unknown which dim → try both
                unavailPrimNames.add(valName.toLowerCase());
                unavailSecNames.add(valName.toLowerCase());
              }
            }
            unavailCount++;
          }
          if (dimArr.length > 0)
            console.log(`[scraper] sortedDimValues[${dimKey}]: ${dimArr.length} values, ${unavailCount} UNAVAILABLE`);
        }
        console.log(`[scraper] sortedDimValues OOS: ${unavailableAsins.size} ASINs, ${unavailPrimNames.size} prim names, ${unavailSecNames.size} sec names`);
      }
    } catch(e) { console.warn('[scraper] sortedDimValues parse err:', e.message); }

// Determine which numeric index in dtaMap codes maps to which dimension
    // Method 1: dimOrder (reliable when populated and contains our dims)
    // Method 2: Range detection — max index at each code position ≈ dim.length-1
    //           ZELUS: maxByPos=[2,7], size(3)→pos0, color(8)→pos1 → pIdx=1
    //           WIHOLL: maxByPos=[18,5], color(19)→pos0, size(6)→pos1 → pIdx=0
    // Method 3: varVals key order (fallback for ambiguous — all dims same length)
    const _vvKeys1 = varVals ? Object.keys(varVals) : [];
    let _posToKey1 = {}; // always defined so 3-dim compound key logic can reference it
    let pIdx, sIdx, _idxMethod1 = 'fallback';
    if (dimOrder.length > 0 && (dimOrder.indexOf(primaryDim.key) >= 0 || dimOrder.indexOf(secondaryDim?.key) >= 0)) {
      pIdx = dimOrder.indexOf(primaryDim.key) >= 0 ? dimOrder.indexOf(primaryDim.key) : 0;
      sIdx = secondaryDim ? (dimOrder.indexOf(secondaryDim.key) >= 0 ? dimOrder.indexOf(secondaryDim.key) : (pIdx===0?1:0)) : (pIdx===0?1:0);
      _idxMethod1 = 'dimOrder';
    } else {
      // Range detection: find max index at each code position, match to dim length
      const _dtaCodes1 = Object.keys(dtaMap);
      if (_dtaCodes1.length > 0) {
        const _numPos1 = _dtaCodes1[0].split('_').length;
        const _maxByPos1 = Array(_numPos1).fill(0);
        for (const _c1 of _dtaCodes1) { _c1.split('_').map(Number).forEach((_v1,_i1)=>{ if(_v1>_maxByPos1[_i1]) _maxByPos1[_i1]=_v1; }); }
        _posToKey1 = {};
        const _usedKeys1 = new Set();
        for (let _pos1=0; _pos1<_numPos1; _pos1++) {
          const _maxVal1 = _maxByPos1[_pos1];
          let _best1=null, _bestDiff1=999;
          for (const [_k1,_v1] of Object.entries(varVals||{})) {
            if (_usedKeys1.has(_k1)) continue;
            const _len1 = Array.isArray(_v1) ? _v1.length : 0;
            const _diff1 = Math.abs(_maxVal1 - (_len1-1));
            if (_diff1 < _bestDiff1) { _bestDiff1=_diff1; _best1=_k1; }
          }
          if (_best1 && _bestDiff1 < 999) { _posToKey1[_pos1] = _best1; _usedKeys1.add(_best1); } // no threshold — pick closest match always
        }
        const _pR1 = Object.entries(_posToKey1).find(([,_v1])=>_v1===primaryDim.key)?.[0];
        const _sR1 = secondaryDim ? Object.entries(_posToKey1).find(([,_v1])=>_v1===secondaryDim.key)?.[0] : undefined;
        if (_pR1 !== undefined) {
          pIdx = parseInt(_pR1);
          sIdx = _sR1 !== undefined ? parseInt(_sR1) : (pIdx===0?1:0);
          _idxMethod1 = 'range';
        }
      }
      if (pIdx === undefined) {
        // Fallback: varVals key order
        pIdx = _vvKeys1.indexOf(primaryDim.key) >= 0 ? _vvKeys1.indexOf(primaryDim.key) : 0;
        sIdx = secondaryDim ? (_vvKeys1.indexOf(secondaryDim.key) >= 0 ? _vvKeys1.indexOf(secondaryDim.key) : (pIdx===0?1:0)) : (pIdx===0?1:0);
        _idxMethod1 = 'varVals';
      }
    }
    console.log(`[scraper] dtaMap pIdx=${pIdx}(${primaryDim.key}) sIdx=${sIdx}(${secondaryDim?.key||'none'}) method=${_idxMethod1}`);

    // For 3+ dim listings, build a compound secondary key from all non-primary dims
    // e.g. size="Small", fit="28in" → secondaryVal="Small / 28in"
    // This preserves all variants without collision
    const _extraDims = allDims.filter(d => d !== primaryDim && d !== secondaryDim);
    const _hasExtraDims = _extraDims.length > 0;
    const comboAsin = {};      // "PrimaryVal|SecondaryVal" → ASIN
    const primaryToAsins = {}; // primaryVal → [ASINs]
    for (const [code, asin] of Object.entries(dtaMap)) {
      const parts = code.split('_').map(Number);
      const pVal  = primaryVals[parts[pIdx]];
      if (!pVal) continue;
      let sVal = secondaryDim ? (secondaryVals[parts[sIdx]] ?? '') : '';
      // Compound secondary: append extra dim values to avoid collisions in 3-dim listings
      // Pack-size dims are price drivers — skip them, don't append to secondary key
      if (_hasExtraDims) {
        for (const extraDim of _extraDims) {
          if (_isPackSizeDim(extraDim)) continue; // price driver — handled by size-price fallback
          const extraIdx = dimOrder.indexOf(extraDim.key) >= 0
            ? dimOrder.indexOf(extraDim.key)
            : Object.entries(typeof _posToKey1 !== 'undefined' ? _posToKey1 : {}).find(([,v])=>v===extraDim.key)?.[0]
            ?? allDims.indexOf(extraDim);
          const extraVal = extraDim.values[parts[parseInt(extraIdx)]] ?? '';
          if (extraVal) sVal = sVal ? `${sVal} / ${extraVal}` : extraVal;
        }
      }
      const key = `${pVal}|${sVal}`;
      comboAsin[key] = asin;
      if (!primaryToAsins[pVal]) primaryToAsins[pVal] = [];
      if (!primaryToAsins[pVal].includes(asin)) primaryToAsins[pVal].push(asin);
    }

    // Fallback 1: rebuild comboAsin from sortedDimValues _asinByIdx (already parsed above)
    // Works when dtaMap absent from server HTML — uses defaultAsin per dim value
    if (!Object.keys(comboAsin).length) {
      // Each dim in allDims may have _asinByIdx if bootstrapped from sortedDimValues
      const _primaryAsinByIdx = primaryDim?._asinByIdx || {};
      const _secAsinByIdx     = secondaryDim?._asinByIdx || {};

      if (Object.keys(_primaryAsinByIdx).length) {
        for (const [_idx, _asin] of Object.entries(_primaryAsinByIdx)) {
          const _pVal = primaryVals[Number(_idx)] || String(_idx);
          if (secondaryDim && secondaryVals.length) {
            // One ASIN per color — use same ASIN for all sizes (best we can do without dtaMap)
            for (const _sVal of secondaryVals) {
              const _key = `${_pVal}|${_sVal}`;
              if (!comboAsin[_key]) comboAsin[_key] = _asin;
            }
          } else {
            comboAsin[`${_pVal}|`] = _asin;
          }
          if (!primaryToAsins[_pVal]) primaryToAsins[_pVal] = [];
          if (!primaryToAsins[_pVal].includes(_asin)) primaryToAsins[_pVal].push(_asin);
        }
        if (Object.keys(comboAsin).length) console.log(`[scraper] built ${Object.keys(comboAsin).length} combos from sortedDimValues (${Object.keys(_primaryAsinByIdx).length} primary ASINs)`);
      } else {
        // Final fallback: re-parse sortedDimValues for primary dim
        try {
          const sddIdx2 = html.indexOf('"sortedDimValuesForAllDims"');
          if (sddIdx2 > -1 && primaryDim) {
            const dI2 = html.indexOf(`"${primaryDim.key}"`, sddIdx2);
            if (dI2 > 0 && dI2 < sddIdx2 + 2000000) { // search entire HTML
              let ai2 = html.indexOf('[', dI2), dep2 = 0, st2 = ai2;
              for (; ai2 < html.length && ai2 < st2+500000; ai2++) {
                if (html[ai2]==='[') dep2++; else if (html[ai2]===']') { dep2--; if(!dep2) break; }
              }
              const dimArr2 = JSON.parse(html.slice(st2, ai2+1));
              for (const e2 of dimArr2) {
                if (!e2.defaultAsin) continue;
                const idx2 = e2.indexInDimList ?? 0;
                const pVal2 = primaryVals[idx2] || e2.dimensionValueDisplayText || String(idx2);
                if (secondaryVals.length) {
                  for (const sVal of secondaryVals) {
                    if (!comboAsin[`${pVal2}|${sVal}`]) comboAsin[`${pVal2}|${sVal}`] = e2.defaultAsin;
                  }
                } else {
                  comboAsin[`${pVal2}|`] = e2.defaultAsin;
                }
                if (!primaryToAsins[pVal2]) primaryToAsins[pVal2] = [];
                if (!primaryToAsins[pVal2].includes(e2.defaultAsin)) primaryToAsins[pVal2].push(e2.defaultAsin);
              }
              if (Object.keys(comboAsin).length) console.log(`[scraper] built ${Object.keys(comboAsin).length} combos from sortedDimValues re-parse`);
            }
          }
        } catch(e2) { console.warn('[scraper] sortedDimValues fallback:', e2.message); }
      }
    }

    // Fallback 2: extract from color→ASIN patterns in HTML (handles some edge cases)
    if (!Object.keys(comboAsin).length) {
      const { colorToAsin: ctaMap } = extractColorAsinMaps(html);
      for (const [c, asin] of Object.entries(ctaMap)) {
        comboAsin[`${c}|`] = asin;
        primaryToAsins[c] = [asin];
      }
    }

    // Detect listing type based on unique ASINs vs combos (moved here — comboAsin now fully populated)
    const uniqueAsinCount = new Set(Object.values(comboAsin)).size;
    const totalCombos = Object.keys(comboAsin).length;
    const isFullyMultiAsin = totalCombos > 0 && uniqueAsinCount >= totalCombos * 0.8;
    const isColorKeyed = !isFullyMultiAsin && uniqueAsinCount <= primaryVals.length * 1.2;
    const isSizeKeyed = !isFullyMultiAsin && !isColorKeyed;
    // Sanitize comboAsin keys — strip invisible chars AND pack suffixes from color part
    const _cleanedCombo = {};
    for (const [key, asin] of Object.entries(comboAsin)) {
      // Strip invisible chars
      let cleanKey = key.replace(/[​‌‍﻿­⁠]/g, '');
      // Strip pack suffixes from primary dim part (before the |)
      if (varVals?.number_of_items) {
        const parts = cleanKey.split('|');
        parts[0] = parts[0].replace(/\s*\(\d+-?\s*pack\)/gi, '').trim();
        cleanKey = parts.join('|');
      }
      // Last-write-wins: if two keys clean to the same value, keep the first one seen
      if (!_cleanedCombo[cleanKey]) _cleanedCombo[cleanKey] = asin;
    }
    for (const key of Object.keys(comboAsin)) delete comboAsin[key];
    Object.assign(comboAsin, _cleanedCombo);

    console.log(`[scraper] listing type: ${isFullyMultiAsin ? 'FULLY-MULTI-ASIN' : isColorKeyed ? 'COLOR-KEYED' : 'SIZE-KEYED'} (${uniqueAsinCount} unique ASINs / ${totalCombos} combos)`);

    // Strategy:
    //   0. Extract swatch prices from main HTML (no extra fetches — fast, works even when blocked)
    //   1. Get baseInStock from main page buy-box (reliable, current selection)
    //   2. Fetch per-ASIN pages for price + stock (fills gaps from step 0)
    //   3. For stock: ONLY mark OOS if buy-box EXPLICITLY says "currently unavailable"
    //      If blocked/ambiguous → fall back to baseInStock (main page result)
    //   4. This avoids false OOS from the server's location + ads on page

    // ── Step 0: Extract swatch prices from embedded page HTML ─────────────────
    // Amazon renders per-ASIN prices in the swatch HTML as:
    //   <span data-asin="ASIN"...>$X.XX with N percent savings</span>
    // This is server-rendered and available without any extra fetches.
    // Also catches the "priceToPay" label pattern: "$X.XX with N percent savings"
    const swatchAsinPrice = {}; // ASIN → price from HTML swatch accessibility labels
    // Pattern: "data-asin=ASIN ... $X.XX with N percent savings" (server-rendered, no extra fetches)
    const _swatchRe1 = /data-asin="([A-Z0-9]{10})"[\s\S]{0,800}?\$([\d.]+)\s+with\s+\d+\s*percent\s+savings/g;
    for (const m of html.matchAll(_swatchRe1)) {
      const p = parseFloat(m[2]);
      if (p > 0 && p < 10000 && !swatchAsinPrice[m[1]]) swatchAsinPrice[m[1]] = p;
    }
    // Pattern 2: savings label appears before data-asin in DOM
    const _swatchRe2 = /\$([\d.]+)\s+with\s+\d+\s*percent\s+savings[\s\S]{0,500}?data-asin="([A-Z0-9]{10})"/g;
    for (const m of html.matchAll(_swatchRe2)) {
      const p = parseFloat(m[1]);
      if (p > 0 && p < 10000 && !swatchAsinPrice[m[2]]) swatchAsinPrice[m[2]] = p;
    }
    if (Object.keys(swatchAsinPrice).length)
      console.log('[scraper] swatch prices: ' + Object.keys(swatchAsinPrice).length + ' ASINs from HTML');

    // baseInStock: what the main page currently shows for the selected variant
    const bbMain = (html.match(/id="availability"[\s\S]{0,3000}/)?.[0] || '')
                 + (html.match(/id="addToCart_feature_div"[\s\S]{0,1000}/)?.[0] || '');
    const mainInStock = bbMain.length > 30
      ? !isAmazonOOS(bbMain)
      : !html.includes('id="outOfStock"') && !html.includes('Currently unavailable');

    // Pick a "fullDim" primary value to fetch per-secondary prices from
    // Prefer the one with most secondary values mapped (likely full coverage)
    const fullPrimary = Object.entries(primaryToAsins)
      .sort((a,b) => b[1].length - a[1].length)[0]?.[0] || primaryVals[0];

    // ── Price + stock per ASIN ────────────────────────────────────────────────
    // Priority: browser clientAsinData (real IP) > swatch prices (from HTML) > server fetch (last resort)
    // Goal: server never fetches Amazon if browser already did it.
    const asinInStock   = {};
    const asinPrice     = {};
    const asinShipping  = {}; // per-ASIN shipping cost (0 = free, null = unknown)
    const asinImgCache  = {}; // img per ASIN cached during price fetch — no extra requests needed

    // Pre-fill from swatch prices (free, zero fetches)
    for (const [asin, price] of Object.entries(swatchAsinPrice)) {
      asinPrice[asin] = price;
    }

    // Browser prefetched data — highest priority, always overrides swatch
    const _clientDataSize = clientAsinData ? Object.keys(clientAsinData).length : 0;
    if (_clientDataSize > 0) {
      for (const [asin, data] of Object.entries(clientAsinData)) {
        if (data.price > 0) asinPrice[asin] = data.price;
        if (data.inStock !== undefined) asinInStock[asin] = data.inStock;
      }
      console.log(`[scraper] browser provided ${_clientDataSize} ASIN prices — skipping server Amazon fetches`);
    }

    // Server-side fetch ONLY for ASINs the browser didn't cover
    // If browser sent clientAsinData, trust it fully — don't second-guess with server fetches
    const _allUniqueAsins = [...new Set(Object.values(comboAsin))].filter(Boolean);

    // If clientAsinData has 'locked' entries → revise mode: fetch ONLY those exact ASINs
    // This ensures revise updates the same variants that were originally pushed, not a random new sample
    const _lockedAsinSet = _clientDataSize > 0
      ? new Set(Object.entries(clientAsinData).filter(([,v]) => v?.locked).map(([a]) => a))
      : new Set();
    const _isLockedMode = _lockedAsinSet.size > 0;
    if (_clientDataSize > 0) console.log(`[scraper] clientAsinData: ${_clientDataSize} entries, locked: ${_lockedAsinSet.size}, isLockedMode: ${_isLockedMode}`);

    const _toFetchAsins   = _clientDataSize > 0 && !_isLockedMode
      ? [] // browser provided real prices — server does nothing
      : _isLockedMode
        ? [..._lockedAsinSet].filter(a => !unavailableAsins.has(a)) // locked: fetch only pushed ASINs
        : _allUniqueAsins.filter(a => !asinPrice[a] && !unavailableAsins.has(a));

    if (_isLockedMode) {
      console.log(`[scraper] revise mode — fetching ${_toFetchAsins.length} locked ASINs (originally pushed)`);
    }

    // Cap at 100 ASINs — spread evenly across all dimension values so every
    // color, size, fit_type etc gets at least one price point fetched.
    const _MAX_FETCH_ASINS = 100; // keep under budget: 100 ASINs ~20s + eBay ops ~60s = safe within 260s
    if (!_isLockedMode && _toFetchAsins.length > _MAX_FETCH_ASINS) {
      console.warn(`[scraper] ${_toFetchAsins.length} ASINs — trimming to ${_MAX_FETCH_ASINS} spread across dimensions`);
      const _sampled = new Set();
      // Pass 1: one ASIN per unique value of each dimension position
      // comboAsin keys are "pVal|sVal" — cover every unique pVal and every unique sVal
      const _seenByPos = {}; // pos → Set of seen values
      for (const [key, asin] of Object.entries(comboAsin)) {
        const parts = key.split('|');
        for (let _pi = 0; _pi < parts.length; _pi++) {
          if (!_seenByPos[_pi]) _seenByPos[_pi] = new Set();
          if (parts[_pi] && !_seenByPos[_pi].has(parts[_pi])) {
            _seenByPos[_pi].add(parts[_pi]);
            _sampled.add(asin);
          }
        }
        if (_sampled.size >= _MAX_FETCH_ASINS) break;
      }
      // Pass 2: fill remaining slots evenly from the full list
      if (_sampled.size < _MAX_FETCH_ASINS) {
        const _step = Math.max(1, Math.floor(_toFetchAsins.length / (_MAX_FETCH_ASINS - _sampled.size)));
        for (let _i = 0; _i < _toFetchAsins.length && _sampled.size < _MAX_FETCH_ASINS; _i += _step) {
          _sampled.add(_toFetchAsins[_i]);
        }
      }
      _toFetchAsins.length = 0;
      for (const a of _sampled) _toFetchAsins.push(a);
      console.log(`[scraper] trimmed to ${_toFetchAsins.length} ASINs (covering ${Object.keys(_seenByPos).length} dim positions)`);
    }
    if (_toFetchAsins.length > 0) {
      console.log(`[scraper] server-fetching ${_toFetchAsins.length} ASINs (no browser data provided)`);
    }

    const _fetchStart = Date.now();
    const _FETCH_BUDGET_MS = 180000; // 180s max for Amazon fetches

    for (let _fi = 0; _fi < _toFetchAsins.length; _fi += 2) {
      if (Date.now() - _fetchStart > _FETCH_BUDGET_MS) {
        console.warn(`[scraper] fetch budget exceeded — stopping at ${_fi}/${_toFetchAsins.length} ASINs (${Math.round((Date.now()-_fetchStart)/1000)}s)`);
        break;
      }
      // Inter-batch gap: randomized 3-7s between pairs — mimics human browsing rhythm
      if (_fi > 0) await sleep(3000 + Math.random() * 4000);
      const _batchResults1 = []; // batch summary: {asin, price, status}
      await Promise.all(_toFetchAsins.slice(_fi, _fi + 2).map(async (asin, _batchIdx) => {
        // Stagger: 2 concurrent, second fires 600-1200ms after first — avoids burst detection
        if (_batchIdx > 0) await sleep(800 + Math.random() * 700);
        let h = await fetchPage(`https://www.amazon.com/dp/${asin}?th=1&psc=1`, ua, 1);

        if (!h) {
          // Blocked — mark OOS (qty=0)
          asinInStock[asin] = false;
          _batchResults1.push({asin, status:'blocked'});
          return;
        }
        // Verify page ASIN matches what we requested — Amazon sometimes redirects
        const pageAsin = h.match(/"ASIN"\s*:\s*"([A-Z0-9]{10})"/)?.[1]
                      || h.match(/name="ASIN"\s+value="([A-Z0-9]{10})"/)?.[1];
        if (pageAsin && pageAsin !== asin) {
          console.warn(`[scraper] ASIN mismatch: requested ${asin}, got ${pageAsin} — price unreliable`);
          return; // skip — wrong page
        }
        const price    = extractPriceFromBuyBox(h);
        const shipping = extractShippingFromPage(h);
        const delivDays = extractDeliveryDays(h);

        // Filter: skip ASINs with delivery > 10 days — too slow for dropshipping
        const _MAX_DELIVERY_DAYS = 10;
        if (delivDays !== null && delivDays > _MAX_DELIVERY_DAYS) {
          asinInStock[asin] = false;
          delete asinPrice[asin];
          _batchResults1.push({asin, status:'slow_delivery', days: delivDays});
          console.warn(`[scraper] ${asin} → skipped (delivery ${delivDays}d > ${_MAX_DELIVERY_DAYS}d limit)`);
          return;
        }

        // Prime filter: when primeOnly=true, skip ASINs that don't have Prime badge
        if (primeOnly && !extractIsPrime(h)) {
          asinInStock[asin] = false;
          delete asinPrice[asin];
          _batchResults1.push({asin, status:'non_prime'});
          console.log(`[scraper] ${asin} → skipped (not Prime, primeOnly mode)`);
          return;
        }

        if (price) { asinPrice[asin] = price; asinInStock[asin] = true; _batchResults1.push({asin, price, status:'ok', delivDays}); }
        else { _batchResults1.push({asin, status:'no_price'}); }
        if (shipping !== null) asinShipping[asin] = shipping; // store per-ASIN shipping cost
        // Cache main image while page is in memory — avoids second fetch for non-color dims
        if (!asinImgCache[asin]) { const _imgs = extractAllImages(h); if (_imgs.length) asinImgCache[asin] = _imgs; }
        const bb = (h.match(/id="availability"[\s\S]{0,3000}/)?.[0] || '')
                 + (h.match(/id="addToCart_feature_div"[\s\S]{0,1000}/)?.[0] || '');
        // Price is truth — BUT main-page OOS signals override price.
        // "Currently unavailable" / "Temporarily out of stock" can show a price but no buy box.
        if (unavailableAsins.has(asin)) {
          asinInStock[asin] = false; // OOS confirmed by main page — price irrelevant
          delete asinPrice[asin];   // remove price so it doesn't leak into comboPrices
          console.warn(`[scraper] ${asin} → OOS (unavailableAsins override — price ignored)`);
        } else if (price && isAmazonOOS(bb)) {
          // Has a price but the buy box says OOS — trust the OOS signal
          asinInStock[asin] = false;
          delete asinPrice[asin];
          console.warn(`[scraper] ${asin} → OOS (buy box says temporarily OOS — price $${price} ignored)`);
        } else if (price) {
          asinInStock[asin] = true; // buy-box price confirmed + not OOS → in stock
        } else {
          asinInStock[asin] = false; // no price → qty=0, whether OOS or just unpriced
        }
      }));
      // Batch summary: one line per batch instead of one per ASIN
      const _ok1 = _batchResults1.filter(r=>r.status==='ok');
      const _bl1 = _batchResults1.filter(r=>r.status==='blocked');
      const _np1 = _batchResults1.filter(r=>r.status==='no_price');
      const _sl1 = _batchResults1.filter(r=>r.status==='slow_delivery');
      const _batchNum1 = Math.floor(_fi/2)+1;
      const _totalBatches1 = Math.ceil(_toFetchAsins.length/2);
      let _batchLog1 = `[scraper] batch ${_batchNum1}/${_totalBatches1}:`;
      if (_ok1.length) _batchLog1 += ` ${_ok1.map(r=>`${r.asin}→$${r.price}`).join(' ')}`;
      if (_bl1.length) _batchLog1 += ` | blocked: ${_bl1.map(r=>r.asin).join(' ')}`;
      if (_np1.length) _batchLog1 += ` | no_price: ${_np1.map(r=>r.asin).join(' ')}`;
      if (_sl1.length) _batchLog1 += ` | slow(>${10}d): ${_sl1.map(r=>`${r.asin}(${r.days}d)`).join(' ')}`;
      console.log(_batchLog1);
      if (_fi + 2 < _toFetchAsins.length) await sleep(200); // 200ms between batches of 2
    }

    // sizePrices / primaryPrices kept for backward compat with comboPrices builder below
    const sizePrices    = {};  // sv → price (for comboPrices fallback)
    const primaryPrices = {};  // pv → price
    const secInStock    = {};
    const primaryInStock = {};
    for (const [key, asin] of Object.entries(comboAsin)) {
      const [pv, sv] = key.split('|');
      if (asinPrice[asin] && sv && !sizePrices[sv])    sizePrices[sv]    = asinPrice[asin];
      if (asinPrice[asin] && pv && !primaryPrices[pv]) primaryPrices[pv] = asinPrice[asin];
      if (asinInStock[asin] !== undefined) {
        if (sv) secInStock[sv]    = secInStock[sv]    ?? asinInStock[asin];
        if (pv) primaryInStock[pv]= primaryInStock[pv]?? asinInStock[asin];
      }
    }

    // mainPrice from buy box = selected variant only — NOT used for combo prices
    // Kept only for bundle sanity check below (comparing vs median)
    const mainPrice = product._basePrice || product.price || 0;
    if (isFullyMultiAsin) {
      // No fallback — no price = qty=0, always. Safer than guessing wrong price.
    } else if (secondaryDim) {
      // Standard 2-dim: fill missing secondary prices
      // No mainPrice fallback — secondary vals priced from comboAsin/asinPrice
    } else {
      // 1-dim: fill primary
      // No mainPrice fallback for primaryVals
    }

    // Sanity-check asinPrice: only on fully multi-ASIN listings (each variant = separate product)
    // On these listings a price >1.8x mainPrice means a bundle ASIN snuck in (e.g. carrier+bag set)
    // Skip this check on standard size/color variants which can legitimately cost more
    const uniqueAsinCountForCheck = new Set(Object.values(comboAsin)).size;
    const totalComboCountForCheck = Object.keys(comboAsin).length;
    const isFullyMultiForCheck = uniqueAsinCountForCheck >= totalComboCountForCheck * 0.8 && uniqueAsinCountForCheck > 3;
    if (mainPrice > 0 && isFullyMultiForCheck) {
      // Use median of fetched prices to detect true outlier bundles
      const _allFetched = Object.values(asinPrice).filter(p => p > 0).sort((a,b)=>a-b);
      const _medianP = _allFetched.length ? _allFetched[Math.floor(_allFetched.length/2)] : 0;
      const _refP = _medianP;
      let sanitized = 0;
      const bundleAsins = new Set();
      for (const [asin, price] of Object.entries(asinPrice)) {
        // FIX: ONLY remove if >5x median — upward outliers are bundles (multi-unit sets).
        // Removed the <0.2x low-price check: size-based listings legitimately have a
        // cheapest single-pack variant that can be a tiny fraction of the median
        // (e.g. 11-count at $5.78 vs 130-count at $36.97 — that's real, not a bundle).
        if (price > _refP * 5) {
          console.warn(`[scraper] ASIN ${asin} price $${price} vs median $${_refP} — outlier bundle, removing`);
          delete asinPrice[asin];
          bundleAsins.add(asin);
          sanitized++;
        }
      }
      // Remove bundle ASINs from comboAsin so those variants are excluded entirely
      if (bundleAsins.size > 0) {
        for (const [key, asin] of Object.entries(comboAsin)) {
          if (bundleAsins.has(asin)) {
            delete comboAsin[key];
            console.warn(`[scraper] removed bundle combo: ${key} (ASIN ${asin})`);
          }
        }
      }
      if (sanitized > 0) console.log(`[scraper] removed ${sanitized} bundle ASINs from fully-multi listing`);
    }

    // Build comboInStock + comboPrices
    // Price priority: per-exact-ASIN → per-color (primary) → per-size (secondary) → mainPrice
    // Stock priority: sortedDimValues UNAVAILABLE → per-exact-ASIN → per-color → per-size → mainInStock
    const comboInStock   = {};
    const comboPrices    = {};
    const comboShipping  = {}; // per-combo Amazon shipping cost (0=free)
    for (const [key, asin] of Object.entries(comboAsin)) {
      const [pv, sv] = key.split('|');
      // For fully-multi-ASIN: each ASIN has its own price — never use main page price as fallback
      // Main page shows only the selected variant's price, not other variants
      // If asinPrice is missing (fetch failed/skipped), use primaryPrices or 0
      const _baseAsinPrice = asinPrice[asin] || 0;
      // Include per-ASIN Amazon shipping in the cost basis so markup applies correctly
      // asinShipping[asin]: 0 = free, positive = paid, null/undefined = fall back to product-level shippingCost
      // Simple rule: if Amazon charges shipping for this ASIN → mark OOS so we skip it
      // asinShipping[asin] = 0 = free, positive = paid, null/undefined = unknown (treat as free)
      const _asinShip = asinShipping[asin] ?? 0;
      if (_asinShip > 0) {
        // Has a shipping fee → not profitable to list → treat as OOS
        comboInStock[key] = false;
        console.log(`[scraper] ${asin} marked OOS — Amazon shipping cost $${_asinShip}`);
      }
      comboPrices[key]   = _baseAsinPrice;
      comboShipping[key] = 0; // never add shipping to price — OOS if they charge
      // sortedDimValues UNAVAILABLE = "See all available options" on Amazon = truly OOS.
      // This is the highest-priority signal — mark false immediately.
      if (unavailableAsins.has(asin)) {
        comboInStock[key] = false;
      } else if (asinInStock[asin] !== undefined) {
        comboInStock[key] = asinInStock[asin];
      } else if (!isFullyMultiAsin && primaryInStock[pv] !== undefined) {
        // FIX: FULLY-MULTI-ASIN must NOT inherit primaryInStock/secInStock from sibling combos.
        // Each combo has its own unique ASIN — cascading OOS from a sibling with a bad buy-box
        // falsely marks every in-stock combo of that color/size as OOS.
        // Only COLOR-KEYED / SIZE-KEYED listings legitimately share stock signals across combos.
        comboInStock[key] = primaryInStock[pv];
      } else if (!isFullyMultiAsin && secInStock[sv] !== undefined) {
        comboInStock[key] = secInStock[sv];
      } else {
        // asinInStock is now always set (true if price found, false if not).
        // This branch only fires for combos whose ASIN was never fetched at all.
        // In that case: no price = can't sell = OOS.
        comboInStock[key] = false;
      }
    }
    // Fill missing comboInStock entries: if we fetched an ASIN and it's in asinInStock,
    // apply that result to ALL combos sharing that ASIN (handles size-is-child-ASIN listings).
    for (const [key, asin] of Object.entries(comboAsin)) {
      if (comboInStock[key] === undefined && asinInStock[asin] !== undefined) {
        comboInStock[key] = asinInStock[asin];
      }
    }

    // ── Apply name-based OOS from sortedDimValues ───────────────────────────────
    // For combos whose ASIN wasn't in unavailableAsins (blocked fetches / missing defaultAsin)
    // we fall back to matching by dimension value name. Works for all listing types:
    //   COLOR-KEYED: unavailPrimName matches → ALL sizes for that color = OOS
    //   SIZE-KEYED:  unavailSecName matches  → ALL colors for that size  = OOS
    //   FULLY-MULTI: unavailableAsins already handled it above
    // FIX: skip name-based OOS for FULLY-MULTI-ASIN — ASIN-based is sufficient
    // Name-based cascades ALL sizes of a color as OOS which is wrong when each combo is independent
    if (!isFullyMultiAsin && (unavailPrimNames.size > 0 || unavailSecNames.size > 0)) {
      let nameOosCount = 0;
      // Mark combos in comboAsin
      for (const [key] of Object.entries(comboAsin)) {
        const [pv, sv] = key.split('|');
        const pvMatch = unavailPrimNames.has((pv||'').toLowerCase());
        const svMatch = sv && unavailSecNames.has((sv||'').toLowerCase());
        if (pvMatch || svMatch) {
          comboInStock[key] = false;
          nameOosCount++;
        }
      }
      // Also mark combos for unavailable primary values NOT in comboAsin
      // (they were unavailable and therefore excluded from dimensionToAsinMap)
      if (unavailPrimNames.size > 0) {
        for (const pv of primaryVals) {
          if (!unavailPrimNames.has((pv||'').toLowerCase())) continue;
          for (const sv of (secondaryVals.length ? secondaryVals : [''])) {
            const key = sv ? `${pv}|${sv}` : `${pv}|`;
            if (comboInStock[key] !== false) { comboInStock[key] = false; nameOosCount++; }
          }
        }
      }
      // Mark combos for unavailable secondary values NOT in comboAsin
      if (unavailSecNames.size > 0) {
        for (const sv of secondaryVals) {
          if (!unavailSecNames.has((sv||'').toLowerCase())) continue;
          for (const pv of primaryVals) {
            const key = `${pv}|${sv}`;
            if (comboInStock[key] !== false) { comboInStock[key] = false; nameOosCount++; }
          }
        }
      }
      console.log(`[scraper] name-based OOS: ${nameOosCount} combos marked (${unavailPrimNames.size} prim, ${unavailSecNames.size} sec names)`);
    }
    console.log(`[scraper] comboInStock: ${Object.values(comboInStock).filter(v=>v===false).length} OOS, ` +
                `${Object.values(comboInStock).filter(v=>v===true).length} in-stock, ` +
                `${Object.values(comboInStock).filter(v=>v===undefined).length} unknown`);


    // No price from Amazon = qty 0 on eBay (applies to ALL listing types)
    // Per-ASIN price is the only reliable source — never use main-page price for other variants
    {
      let _noPrice = 0;
      for (const key of Object.keys(comboAsin)) {
        if (!comboPrices[key] || comboPrices[key] <= 0) {
          comboPrices[key] = 0;
          comboInStock[key] = false; // no price → qty 0
          _noPrice++;
        }
      }
      if (_noPrice > 0) console.log(`[scraper] ${_noPrice} combos qty=0 (no price from Amazon)`);
    }
    product.inStock = Object.keys(comboInStock).length > 0
      ? Object.values(comboInStock).some(v => v !== false)
      : mainInStock;

    // Top-level product.price selection — STRICT, no inheritance from sibling variants.
    // Old behavior used "cheapest in-stock variant" as a fallback when the URL ASIN
    // had no fetched price, which produced dangerous mispriced listings: a $79 variant
    // could end up listed at the $14.97 of a different variant whose ASIN happened to
    // succeed at fetch time. Per user rule (non-negotiable): NO PRICE FALLBACK FROM
    // OTHER VARIANTS, EVER. If the URL ASIN's own price isn't available, top-level
    // price stays 0 — which the handlePush AMAZON-$0 guard then converts into a
    // qty=0 unbuyable listing. The buyable variants still publish at their own real
    // prices via buildVariants; only the top-level "starting at" price is gated.
    let _urlAsinPrice = 0;
    if (asin) {
      for (const [key, a] of Object.entries(comboAsin)) {
        if (a === asin) {
          const cp = comboPrices[key];
          if (cp > 0 && comboInStock[key] !== false) { _urlAsinPrice = cp; break; }
        }
      }
    }
    if (_urlAsinPrice > 0) {
      product.price = _urlAsinPrice;
      console.log(`[scraper] product.price=$${_urlAsinPrice} (URL ASIN ${asin} match)`);
    } else {
      // No URL ASIN price — leave product.price as 0. Do NOT inherit from sibling
      // variants. handlePush will detect this and force the listing to qty=0.
      // Single-variant fallback: only use _basePrice if there are NO combos at all
      // (genuine single-product page where no variants exist to inherit from).
      if (Object.keys(comboPrices).length === 0 && product._basePrice > 0) {
        product.price = product._basePrice;
        console.log(`[scraper] product.price=$${product._basePrice} (single-variant from buy-box)`);
      } else {
        product.price = 0;
        console.warn(`[scraper] product.price=0 (URL ASIN ${asin||'(none)'} not fetched, NO sibling-price fallback per safety rule — will publish unbuyable)`);
      }
    }

    // Detect if per-variant price fetches were genuinely blocked.
    // Many products legitimately have identical prices across variants (common for apparel).
    // Only flag _pricesFailed when ALL combo prices equal the main-page price AND
    // each combo has a different ASIN (meaning real per-ASIN data should exist but didn't load).
    const allComboPrices = Object.values(comboPrices).filter(p => p > 0);
    // Per user rule: if Amazon returned a price, use it. Period. No "looks suspicious"
    // false-positive flagging on uniform prices — many legit products have all variants
    // at the same price. _pricesFailed stays false unless prices are literally absent.
    product._pricesFailed = false;

    // ── Per-variation images ──────────────────────────────────────────────────
    // CRITICAL: Amazon's swatchImgMap is a flat {value → url} where the keys are
    // dimension VALUES (e.g. "Black", "Slim Fit", "Vintage Pattern"). The dimension
    // those values belong to is NOT necessarily the primary dimension we picked
    // for the eBay listing's role priority. On real listings the image-driving
    // dim can be color, size, style, pattern, fit_type, or any compound — it's
    // whatever Amazon chose to put swatches on. The old code blindly looked up
    // swatchImgMap[pv] where pv was a primaryDim value, which silently produced
    // 0 matches whenever the image dim != primary dim, then fell back to a
    // round-robin gallery slice that gave every variant essentially random images.
    //
    // Fix: detect the image dim by counting how many keys in swatchImgMap match
    // each variation's values. The variation with the highest match count is the
    // dim Amazon actually uses for image variation. If detection fails (no swatch
    // map at all, or no variation matches), fall back to primaryDim as before.
    const _normVal = s => String(s||'').toLowerCase().replace(/[^a-z0-9]/g,'');
    const _detectImageDim = () => {
      const swatchKeys = Object.keys(swatchImgMap);
      if (!swatchKeys.length) return null;
      const swatchKeysNorm = swatchKeys.map(_normVal);
      let best = null, bestScore = 0;
      for (const dim of effectiveDims) {
        if (!dim?.values?.length) continue;
        let score = 0;
        for (const v of dim.values) {
          const vn = _normVal(v);
          if (!vn) continue;
          // Direct match (or substring — handles compound swatch alts like "Black 2-Pack" matching "Black")
          if (swatchKeysNorm.some(sk => sk === vn || sk.includes(vn) || vn.includes(sk))) score++;
        }
        // Require at least 2 matches OR >=50% coverage to count as "this dim drives images"
        const coverage = score / dim.values.length;
        const qualifies = score >= 2 || (score >= 1 && coverage >= 0.5);
        if (qualifies && score > bestScore) {
          best = dim;
          bestScore = score;
        }
      }
      if (best) console.log(`[images] detected image dim: "${best.name}" (${bestScore}/${best.values.length} swatch matches)`);
      else      console.log(`[images] no clear image dim from ${swatchKeys.length} swatches — defaulting to primary "${primaryDim?.name}"`);
      return best;
    };
    const imageDim = _detectImageDim() || primaryDim;
    const imageDimVals = imageDim.values || primaryVals;
    // Build a normalized swatch lookup so values like "Black 2x6" still find "Black"
    const _swatchByNorm = {};
    for (const [k, v] of Object.entries(swatchImgMap)) {
      const kn = _normVal(k);
      if (kn && !_swatchByNorm[kn]) _swatchByNorm[kn] = v;
    }
    const _findSwatch = val => {
      const vn = _normVal(val);
      if (!vn) return null;
      if (_swatchByNorm[vn]) return _swatchByNorm[vn];
      // Try substring matches (compound swatch alts)
      for (const [kn, url] of Object.entries(_swatchByNorm)) {
        if (kn.includes(vn) || vn.includes(kn)) return url;
      }
      return null;
    };

    // Build per-image-dim image map (was: per-primary-dim, hardcoded)
    const colorImgMap = {};
    for (const iv of imageDimVals) {
      const swatch = _findSwatch(iv);
      colorImgMap[iv] = swatch ? [swatch] : [];
    }
    // Fetch images for image-dim values that don't have swatch images.
    // Look up an ASIN whose combo includes this image-dim value (could be on
    // either side of the | delimiter since image dim isn't always primary).
    const needImg = imageDimVals.filter(iv => !colorImgMap[iv]?.length);
    if (needImg.length) {
      // Pass 1: use asinImgCache (already extracted during price fetch — free, no extra requests)
      for (const iv of needImg) {
        // Find any combo whose key contains this image-dim value
        const ivn = _normVal(iv);
        const _ivAsin = Object.entries(comboAsin).find(([k]) => {
          const parts = k.split('|').map(_normVal);
          return parts.some(p => p === ivn);
        })?.[1];
        if (_ivAsin && asinImgCache[_ivAsin]) colorImgMap[iv] = asinImgCache[_ivAsin];
      }
      // Pass 2: fetch pages only for image-dim values still missing images
      const stillNeedImg = needImg.filter(iv => !colorImgMap[iv]?.length);
      if (stillNeedImg.length) {
        await Promise.all(stillNeedImg.slice(0, 8).map(async iv => {
          const ivn = _normVal(iv);
          const asin = Object.entries(comboAsin).find(([k]) => {
            const parts = k.split('|').map(_normVal);
            return parts.some(p => p === ivn);
          })?.[1];
          if (!asin) return;
          const h = await fetchPage(`https://www.amazon.com/dp/${asin}`, ua);
          if (h) { const imgs = extractAllImages(h); if (imgs.length) colorImgMap[iv] = imgs; }
        }));
      }
    }
    // Fallback: assign from full hiRes pool for image-dim values still missing
    let fi = 0;
    const imgPool = allHiRes.length > 0 ? allHiRes : product.images;
    for (const iv of imageDimVals) {
      if (!colorImgMap[iv]?.length) colorImgMap[iv] = [imgPool[fi++ % Math.max(1, imgPool.length)] || ''];
    }

    // product.images stays as the default color's 12 photos (set by extractVariantImages earlier)
    // This gives eBay a full 12-photo gallery for the main listing
    // Per-color images are stored in colorImgMap/variationImages for per-variant imageUrls
    // Ensure product.images is not empty
    if (!product.images?.length) {
      product.images = Object.values(colorImgMap).filter(Boolean).slice(0, 12);
    }
    console.log(`[images] product.images: ${product.images.length} (default color gallery)`);

    // ── Build variation groups ────────────────────────────────────────────────
    // Primary dimension group (e.g. Color)
    product.variations.push({
      name: primaryDim.name,
      values: primaryVals.map(pv => {
        const inStock = Object.entries(comboInStock)
          .some(([k,v]) => k.startsWith(`${pv}|`) && v !== false);
        const pvPrices = Object.entries(comboPrices)
          .filter(([k]) => k.startsWith(`${pv}|`)).map(([,p]) => p).filter(p=>p>0);
        return {
          value:   pv,
          price:   pvPrices.length ? Math.min(...pvPrices) : 0,
          image:   colorImgMap[pv] || '',
          inStock: inStock,
          enabled: inStock,
        };
      }),
    });
    // Secondary dimension group (e.g. Size, Style)
    if (secondaryDim) {
      product.variations.push({
        name: secondaryDim.name,
        values: secondaryVals.map(sv => {
          const inStock = Object.entries(comboInStock)
            .some(([k,v]) => k.endsWith(`|${sv}`) && v !== false);
          return {
            value:   sv,
            // Find any combo price that includes this secondary value (for display)
            price:   Object.entries(comboPrices).find(([k]) => k.endsWith(`|${sv}`))?.[1] || 0,
            inStock: inStock,
            enabled: inStock,
            image:   '',
          };
        }),
      });
    }

    // Store on product
    // ── Explicit OOS for combos absent from dimensionToAsinMap ───────────────
    // Amazon only includes in-stock combos in dimensionToAsinMap.
    // For fully-multi-ASIN listings: Amazon's blocked page may show fewer combos
    // than are actually available — don't falsely mark as OOS.
    const _svList = secondaryDim ? secondaryVals : [''];
    let explicitOosCount = 0;
    if (!isFullyMultiAsin) {
      // Only apply explicit OOS for non-fully-multi-ASIN listings
      // (where dtaMap is reliable even when blocked)
      for (const pv of primaryVals) {
        for (const sv of _svList) {
          const key = `${pv}|${sv}`;
          if (!comboAsin[key] && comboInStock[key] === undefined) {
            comboInStock[key] = false;
            if (!comboPrices[key]) comboPrices[key] = 0; // absent = no price data
            explicitOosCount++;
          }
        }
      }
      if (explicitOosCount > 0)
        console.log(`[scraper] explicit OOS (absent from dtaMap): ${explicitOosCount} combos`);
    } else {
      // Fully multi-ASIN: absent combos = Amazon blocked (incomplete dtaMap) → in-stock, price unknown
      for (const pv of primaryVals) {
        for (const sv of _svList) {
          const key = `${pv}|${sv}`;
          if (comboInStock[key] === undefined) comboInStock[key] = true;
          if (!comboPrices[key]) comboPrices[key] = 0; // no mainPrice fallback — each ASIN has unique price
        }
      }
    }

    product.comboAsin     = comboAsin;
    product.comboInStock  = comboInStock;
    product.sizePrices    = sizePrices;
    product.comboPrices   = comboPrices;
    product.comboShipping = comboShipping; // per-ASIN Amazon shipping cost
    // Store variationImages under the DETECTED image dim, not blindly under primaryDim.
    // imageDim was set above by _detectImageDim() — it's whichever variation Amazon's
    // swatch alts actually match. Storing under the wrong dim is the root cause of
    // wrong-image bugs on style/pattern/fit_type listings.
    product.variationImages[imageDim.name] = Object.fromEntries(
      imageDimVals.map(iv => [iv, Array.isArray(colorImgMap[iv]) ? colorImgMap[iv] : (colorImgMap[iv] ? [colorImgMap[iv]] : [])]).filter(([,imgs]) => imgs.length)
    );
    // Store per-ASIN images so push can map images to exact (Fit_type+Color) combos
    // Each unique ASIN has its own images; for 3-dim listings multiple combos share an ASIN
    product.asinImages = {};
    for (const [asin, imgs] of Object.entries(asinImgCache)) {
      if (imgs && (Array.isArray(imgs) ? imgs.length : imgs)) {
        product.asinImages[asin] = Array.isArray(imgs) ? imgs : [imgs];
      }
    }
    // Store primary/secondary dim names for push/revise to use
    product._primaryDimName   = primaryDim.name;
    product._secondaryDimName = secondaryDim?.name || null;
    product._extraDimNames    = _extraDims.map(d => d.name); // all extra dims beyond primary+secondary
    // Store the detected image-driving dim name explicitly. push and rebuild-photos
    // use this to know which variationImages map to read from. Falls back to
    // primaryDim.name when detection found no clear winner.
    product._imageDimName     = imageDim.name;
    // FIX: store listing type so buildVariants can make correct qty decisions
    product._isFullyMultiAsin = isFullyMultiAsin;
    // Store OOS ASINs from sortedDimValues — survives even when comboAsin is empty (dtaMap blocked)
    // handleRevise uses this to restore in-stock combos without a second page fetch
    // Always store — empty array means sortedDimValues ran with 0 OOS (all combos available)
    // undefined means sortedDimValues never ran (different from 0-OOS)
    product._unavailableAsins = unavailableAsins?.size > 0 ? [...unavailableAsins] : [];

    // ── HARD CAP: 50 ASINs MAX, VISIBLE/REAL ONLY ─────────────────────────────
    // Per user rule: every multi-variation product is capped at 50 ASINs total.
    // Combos must be (a) priced, (b) in stock at scrape time, (c) backed by a
    // real ASIN. Phantom dim combos (Amazon UI shows them but no ASIN exists)
    // and OOS-at-import combos are dropped entirely so smartSync never sees
    // them and never tries to fetch them on later cycles. The kept set becomes
    // the IMMUTABLE variant universe for this product — additions Amazon makes
    // later are ignored, removals are reflected via OOS in the normal sync flow.
    {
      const MAX_ASINS = 50;
      // Build the "real visible" combo list, ranked by likelihood of being a
      // good listing variant: in-stock + priced > 0 first, then by ASIN order
      // for stable selection across runs.
      const _visibleCombos = Object.entries(product.comboAsin || {})
        .filter(([key, asin]) => {
          if (!asin) return false; // no ASIN → phantom combo, drop
          if (product.comboInStock?.[key] === false) return false; // explicit OOS at import → drop
          const p = parseFloat(product.comboPrices?.[key]) || 0;
          if (p <= 0) return false; // no price → can't list, drop
          return true;
        });

      // If more than 50 real visible combos exist, take the cheapest 50
      // (cheapest = most likely to convert; deterministic; user-friendly default)
      let _kept;
      if (_visibleCombos.length > MAX_ASINS) {
        _kept = _visibleCombos
          .sort((a, b) => (parseFloat(product.comboPrices[a[0]]) || 0) - (parseFloat(product.comboPrices[b[0]]) || 0))
          .slice(0, MAX_ASINS);
        console.log(`[scraper] HARD CAP: trimming ${_visibleCombos.length} real combos → ${MAX_ASINS} (cheapest first)`);
      } else {
        _kept = _visibleCombos;
        console.log(`[scraper] kept all ${_visibleCombos.length} real combos (under ${MAX_ASINS} cap)`);
      }

      const _keepKeys  = new Set(_kept.map(([k]) => k));
      const _keepAsins = new Set(_kept.map(([, a]) => a));

      // Rebuild comboAsin / comboPrices / comboInStock with only the kept set.
      const _newComboAsin = {}, _newComboPrices = {}, _newComboInStock = {};
      for (const [k, a] of _kept) {
        _newComboAsin[k]    = a;
        _newComboPrices[k]  = product.comboPrices[k];
        _newComboInStock[k] = true; // we already filtered to in-stock above
      }
      product.comboAsin    = _newComboAsin;
      product.comboPrices  = _newComboPrices;
      product.comboInStock = _newComboInStock;

      // Trim variation values too — drop any dim value whose name doesn't
      // appear in any kept combo key. This keeps the eBay variation dropdown
      // honest (shows only colors/sizes that have a real kept variant).
      if (Array.isArray(product.variations)) {
        // Collect all dim values that appear on the primary/secondary side of any kept key
        const _keptPrimVals = new Set();
        const _keptSecVals  = new Set();
        for (const k of _keepKeys) {
          const [pVal, sValRaw] = k.split('|');
          if (pVal) _keptPrimVals.add(pVal);
          if (sValRaw) _keptSecVals.add(sValRaw.split(' / ')[0]); // strip compound suffix
        }
        product.variations = product.variations
          .map(vg => {
            const isPrim = vg.name === product._primaryDimName;
            const isSec  = vg.name === product._secondaryDimName;
            if (!isPrim && !isSec) return vg; // leave extra dims untouched
            const _allowed = isPrim ? _keptPrimVals : _keptSecVals;
            const filtered = (vg.values || []).filter(v => {
              const vName = typeof v === 'string' ? v : v.value;
              return _allowed.has(vName);
            });
            return { ...vg, values: filtered };
          })
          .filter(vg => (vg.values || []).length > 0); // drop dims that ended up empty
      }

      // Trim variationImages too — drop entries for filtered-out values
      if (product.variationImages && typeof product.variationImages === 'object') {
        for (const [dimName, valMap] of Object.entries(product.variationImages)) {
          if (!valMap || typeof valMap !== 'object') continue;
          const isPrim = dimName === product._primaryDimName;
          const isSec  = dimName === product._secondaryDimName;
          if (!isPrim && !isSec) continue;
          const _allowed = isPrim ? new Set([...new Set([...Object.keys(_newComboAsin)].map(k => k.split('|')[0]))])
                                   : new Set([...new Set([...Object.keys(_newComboAsin)].map(k => (k.split('|')[1]||'').split(' / ')[0]))]);
          for (const valName of Object.keys(valMap)) {
            if (!_allowed.has(valName)) delete valMap[valName];
          }
        }
      }

      // If after trimming the product has 0 combos, set a flag so callers
      // can decide what to do:
      //   - handlePush (first-time push): refuse outright — no point listing
      //     a product with zero buyable variants
      //   - smartSync (existing listing): mark every variant qty=0 but keep
      //     the listing alive so it can recover when Amazon comes back
      // The scraper itself doesn't know which context it's running in, so it
      // just exposes the fact via _zeroVariantsAfterCap and lets the callers
      // do the right thing.
      if (Object.keys(_newComboAsin).length === 0) {
        console.warn(`[scraper] HARD CAP: 0 real combos after filter — flagging _zeroVariantsAfterCap`);
        product._zeroVariantsAfterCap = true;
        // Keep hasVariations + variations as scraped — sync needs them to
        // know which existing eBay variants to mark OOS. Push will refuse
        // before that matters.
      }
    }
  }

  console.log(`[scrapeAmazonProduct] "${product.title?.slice(0,50)}" price=$${product.price} imgs=${product.images.length} hasVar=${product.hasVariations}`);
  return product;
}

// ══════════════════════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════════════════
// PUSH + REVISE — Clean rewrite
// ═══════════════════════════════════════════════════════════════════════════════
//
// PUSH:   Creates a brand-new eBay listing from a scraped Amazon product.
// REVISE: Updates an existing eBay listing in-place (keeps listing ID live).
//
// Handles ALL Amazon listing types:
//   • Simple (no variations)
//   • Color only
//   • Size only
//   • Color + Size
//   • Any other dimension (treated as "size")
//
// Stock logic:
//   If comboAsin map exists → check comboInStock per combo → qty=0 if OOS
//   If no comboAsin          → trust product.inStock for simple / defaultQty for all variants
//
// Image logic:
//   Per-SKU inventory item: [this_color_image]  (just one — eBay shows the right swatch)
//   Group imageUrls:        all product images up to 12
//   If no color image:      use product.images[0]
//
// Price formula: (amazonCost + handling) × (1 + markup/100) / (1 − 0.1335) + 0.30
// ═══════════════════════════════════════════════════════════════════════════════

// ─── Shared helpers ───────────────────────────────────────────────────────────

function makeApplyMk(markupPct, handling) {
  const fee = 0.1335;
  // shipOverride: per-ASIN Amazon shipping cost (replaces global handling for shipping part)
  // Formula: (amazonCost + shipping + handling) × markup / (1 - fee) + 0.30
  return function applyMk(cost, shipOverride) {
    const c = parseFloat(cost) || 0;
    if (c <= 0) return 0;
    const ship = shipOverride != null ? parseFloat(shipOverride) || 0 : 0;
    return Math.max(
      Math.ceil(((c + ship + handling) * (1 + markupPct / 100) / (1 - fee) + 0.30) * 100) / 100, 0
    );
  };
}

function buildDescription(title, bullets, para, aspects) {
  const bulletHtml = (bullets || []).length
    ? '<ul>' + bullets.map(b => `<li>${String(b).replace(/</g,'&lt;').replace(/>/g,'&gt;')}</li>`).join('') + '</ul>'
    : '';
  const specRows = Object.entries(aspects || {})
    .filter(([k, v]) => !['ASIN','UPC','Color','Size','Brand Name','Brand'].includes(k) && v?.[0] && String(v[0]).length < 80)
    .slice(0, 10)
    .map(([k, v]) => `<tr><td><b>${k}</b></td><td>${v[0]}</td></tr>`).join('');
  const specsTable = specRows
    ? `<br/><table border="0" cellpadding="4" cellspacing="0" width="100%"><tbody>${specRows}</tbody></table>`
    : '';
  return [
    `<h2>${title}</h2>`,
    bulletHtml,
    para ? `<p>${para}</p>` : '',
    specsTable,
    '<br/><p style="font-size:11px;color:#888">Ships from US. Item is new. Please message us with any questions before purchasing.</p>',
  ].filter(Boolean).join('\n');
}

// Build a flat variant list from product variations
// Returns array of { sku, dimKey (e.g. "Purple|Queen"), dims {}, price, qty, image }
function buildVariants({ product, groupSku, applyMk, defaultQty, body }) {
  const SKU_MAX  = 50;
  const prefix   = groupSku + '-';
  const maxSuffix = SKU_MAX - prefix.length;

  function mkSku(parts) {
    const raw = parts.join('_').replace(/[^A-Z0-9]/gi, '_').toUpperCase().replace(/_+/g, '_').replace(/^_|_$/g, '');
    if (raw.length <= maxSuffix) return prefix + raw;
    const hash = raw.split('').reduce((h, c) => ((h << 5) - h + c.charCodeAt(0)) | 0, 0);
    const hashStr = (hash >>> 0).toString(16).toUpperCase().padStart(8, '0');
    return prefix + raw.slice(0, maxSuffix - 9) + '_' + hashStr;
  }

  const comboAsin    = product.comboAsin    || {};
  const comboPrices  = product.comboPrices  || {};
  const sizePrices   = product.sizePrices   || {};
  // WS: add estimated shipping to base price (it's part of COGS)
  const _wsShipCost = (product._source === 'webstaurant' && product._wsShipping > 0)
    ? parseFloat(product._wsShipping) : 0;
  const basePrice    = parseFloat(product.price || product.cost || 0) + _wsShipCost;


  const hasComboData = Object.keys(comboAsin).length > 0;

  // Sanitize comboInStock: if ALL entries are false (corrupted by a bad revise)
  // but we have no live sortedDimValues evidence, reset everything to in-stock.
  // Push should always list everything as available unless explicitly OOS.
  const _rawComboInStock = product.comboInStock || {};
  const _allEntries = Object.values(_rawComboInStock);
  const _allFalse = _allEntries.length > 0 && _allEntries.every(v => v === false);
  const _hasSomeFresh = !_allFalse; // if not all-false, trust the data
  const comboInStock = _allFalse
    ? {} // all-false = corrupted → reset to empty (isInStock returns true for missing)
    : _rawComboInStock;
  if (_allFalse) console.log(`[push] comboInStock all-false detected — resetting to in-stock (${_allEntries.length} combos)`);

  // Normalize variation values — Amazon uses [{value:'Red'}], AliExpress uses ['Red']
  // Normalize both to [{value: 'Red'}] so the rest of the code works uniformly
  const _normalizeVarValues = (dim) => {
    if (!dim) return dim;
    return {
      ...dim,
      values: (dim.values || []).map(v =>
        typeof v === 'string' ? { value: v } : (v?.value !== undefined ? v : { value: String(v) })
      ).filter(v => v.value)
    };
  };

  // Use _primaryDimName/_secondaryDimName if available (set by new scraper)
  // Fall back to detecting by name for backward compat with cached products
  const primaryName   = product._primaryDimName   || null;
  const secondaryName = product._secondaryDimName || null;
  const _rawPrimary  = primaryName
    ? product.variations?.find(v => v.name === primaryName)
    // Fallback priority: color > size (3+ vals, no color, with style/pattern) > first
    : product.variations?.find(v => /color|colour/i.test(v.name))
      || (() => {
           const sz = product.variations?.find(v => /^size/i.test(v.name) && (v.values||[]).length >= 3);
           const hasColor = product.variations?.some(v => /color|colour/i.test(v.name));
           const hasStyleOrPattern = product.variations?.some(v => /^style|^pattern/i.test(v.name));
           return (sz && !hasColor && hasStyleOrPattern) ? sz : null;
         })()
      || product.variations?.[0];
  const _rawSecondary = secondaryName
    ? product.variations?.find(v => v.name === secondaryName)
    : product.variations?.find(v => v !== _rawPrimary);
  const primaryGroup   = _normalizeVarValues(_rawPrimary);
  const secondaryGroup = _normalizeVarValues(_rawSecondary);

  // Image map: keyed by primary dimension value
  // Normalize: variationImages may store arrays (Amazon new) or strings (AliExpress)
  const _normalizeImgMap = (m) => {
    const out = {};
    for (const [k,v] of Object.entries(m||{})) out[k] = Array.isArray(v) ? v : (v ? [v] : []);
    return out;
  };
  // Also check AliExpress per-variation .images field (set directly on variation group)
  const _aeVarImgs = (() => {
    const prim = product.variations?.find(v => v.name === (primaryGroup?.name || product._primaryDimName));
    return _normalizeImgMap(prim?.images || {});
  })();
  // Build the per-variant image map. PRIORITY: read from product._imageDimName
  // first (set by the scraper based on which dim Amazon's swatches actually
  // matched), then primaryDim, then the legacy 'Color' key. The old code went
  // 'Color' → primary which was wrong for any listing whose images vary by
  // style/pattern/fit_type/size.
  const _imgDimName = product._imageDimName || primaryGroup?.name || product._primaryDimName;
  const imgMap = Object.assign(
    {},
    // Lowest priority: legacy 'Color' fallback (kept for old products without _imageDimName)
    _normalizeImgMap(product.variationImages?.['Color'] || {}),
    // Mid priority: primary dim's map (in case the scraper guessed wrong about image dim)
    primaryGroup ? _normalizeImgMap(product.variationImages?.[primaryGroup.name] || {}) : {},
    // Highest priority: explicit image dim from the new scraper detection
    _imgDimName ? _normalizeImgMap(product.variationImages?.[_imgDimName] || {}) : {},
    _aeVarImgs, // AliExpress color swatch images (highest, hand-set)
  );

  // FULLY-MULTI-ASIN: no fallback maps needed — blocked combos get qty=0 directly.
  // Each combo has its own unique Amazon price; guessing from siblings is inaccurate.
  const _secPriceFallback = {}; // kept for non-fully-multi use below (not used for FULLY-MULTI)
  const _primPriceFallback = {};

  function getAmazonPrice(pVal, sVal) {
    const key = _resolveComboKey(pVal, sVal); // handles 3-dim compound keys
    if (comboPrices[key] > 0) return comboPrices[key];
    // AliExpress DOM parse: comboPrices is empty — fall back to base product price
    // runParams parse: comboPrices has per-combo prices from skuPriceList
    if (product._source === 'aliexpress' && Object.keys(comboPrices).length === 0) {
      return product.price || 0;
    }
    return 0; // Amazon: no price = can't sell
  }

  // FIX: 3-dim listings use compound secondary keys like "Large / Regular" (size + fit_type).
  // Build a helper that resolves a plain sVal to possible compound keys in comboAsin/comboPrices.
  function _resolveComboKey(pVal, sVal) {
    const plain = `${pVal || ''}|${sVal || ''}`;
    if (comboAsin[plain] || comboPrices[plain]) return plain;
    // Try compound secondary: "pVal|sVal / extraDim"
    const prefix = `${pVal || ''}|${sVal || ''}`;
    const compound = Object.keys(comboAsin).find(k => k === prefix || k.startsWith(prefix + ' / '));
    if (compound) return compound;
    // FIX: when color is a single-value skipped dim, scraper sets size as primary.
    // Try sVal as primary key, or swap primary/secondary.
    if (sVal) {
      const swapped = `${sVal}|`;
      if (comboAsin[swapped] || comboPrices[swapped]) return swapped;
      const swappedFull = `${sVal}|${pVal}`;
      if (comboAsin[swappedFull] || comboPrices[swappedFull]) return swappedFull;
    }
    // FIX: try pVal-only key (single-dim listing where sVal is empty)
    const pvOnly = `${pVal}|`;
    if (comboAsin[pvOnly] || comboPrices[pvOnly]) return pvOnly;
    // FIX: fuzzy pVal match only for single-dim listings (sVal is empty)
    // NEVER fuzzy match when sVal is present — that would map phantom combos to wrong ASINs
    if (!sVal) {
      const pNorm = (pVal||'').toLowerCase().replace(/[^a-z0-9]/g,'');
      const fuzzy = Object.keys(comboAsin).find(k => {
        const [kp, ks] = k.split('|');
        return !ks && (kp||'').toLowerCase().replace(/[^a-z0-9]/g,'') === pNorm;
      });
      if (fuzzy) return fuzzy;
    }
    return plain;
  }

  function isInStock(pVal, sVal) {
    if (!hasComboData) return true; // no combo data → assume in stock
    const key = _resolveComboKey(pVal, sVal);
    if (comboInStock[key] === false) return false; // confirmed OOS / no price
    // FIX: for fully-multi-ASIN listings, a combo not in comboAsin doesn't exist on Amazon
    if (product._isFullyMultiAsin && !comboAsin[key]) {
      return false; // combo doesn't exist on Amazon
    }
    // AliExpress: comboPrices may be empty (DOM parse can't get per-combo prices without clicking)
    // In that case fall back to comboInStock — if explicitly false it's OOS, otherwise assume in stock
    const isAE = product._source === 'aliexpress';
    const noComboPrices = Object.keys(comboPrices).length === 0;
    if (isAE && noComboPrices) {
      return comboInStock[key] !== false; // use AE qty-based stock if available, else true
    }
    // Amazon: no price = can't sell
    const hasPrice = comboPrices[key] > 0;
    if (!hasPrice) return false;
    return true;
  }

  function getImage(pVal, idx) {
    if (!pVal) return product.images?.[0] || '';
    const _toFirst = v => Array.isArray(v) ? v[0] : v;
    const direct = imgMap[pVal];
    if (direct) return _toFirst(direct);
    const ci = Object.entries(imgMap).find(([k]) => k.toLowerCase() === (pVal||'').toLowerCase());
    if (ci) return _toFirst(ci[1]);
    // Cycle through product.images by variant index for non-color dims
    if (product.images?.length > 1 && idx !== undefined) {
      return product.images[idx % product.images.length] || product.images[0] || '';
    }
    return product.images?.[0] || '';
  }

  function getImages(pVal, idx) {
    // Returns ARRAY of images for a primary value (for eBay imageUrls per variant)
    if (!pVal) return product.images?.slice(0, 8) || [];
    const raw = imgMap[pVal]
      || Object.entries(imgMap).find(([k]) => k.toLowerCase() === (pVal||'').toLowerCase())?.[1];
    if (raw) return Array.isArray(raw) ? raw.slice(0, 8) : [raw];
    // Fall back to cycling product.images
    if (product.images?.length > 1 && idx !== undefined) {
      return [product.images[idx % product.images.length] || product.images[0]].filter(Boolean);
    }
    return product.images?.slice(0, 1) || [];
  }

  const variants = [];

  // Check if we have extra dims beyond primary+secondary (3-dim listings like mattresses)
  // If so, iterate comboAsin directly — each unique combo key becomes its own variant.
  // This is the only correct approach: the nested pv×sv loop would miss firmness variants
  // and assign the first matching compound key's price to ALL firmness options.
  // _hasExtraCombos: any compound key exists → this is an N-dim listing
  const _hasExtraCombos = hasComboData && Object.keys(comboAsin).some(k => {
    const sv = k.split('|')[1]; return sv && sv.includes(' / ');
  });

  // For FULLY-MULTI-ASIN listings, ALWAYS iterate comboAsin directly regardless of dims.
  // Cross-product (pv×sv) creates phantom variants — e.g. cat food: 17 flavors × 9 sizes = 153
  // but only 22 real combos exist. 131 phantom variants clutter eBay with qty=0.
  // Reuse _hasExtraCombos flag — override it for FULLY-MULTI-ASIN with any combo data.
  const _useComboDirectPath = _hasExtraCombos || (product._isFullyMultiAsin && hasComboData && primaryGroup && secondaryGroup);

  if (_useComboDirectPath && primaryGroup && secondaryGroup) {
    // FULLY-MULTI-ASIN: iterate comboAsin keys directly — each key = one eBay variant
    // For 3-dim listings: key = "PrimaryVal|SecondaryVal / ExtraVal"
    // We split ExtraVal out as a THIRD eBay dimension instead of compounding into secondary.
    // eBay supports 3+ variation dimensions (array of Specification, no limit in API docs).
    const _pValImgIdx = {};
    let _nextImgIdx = 0;
    const _seen = new Set();

    // Detect 3-dim listings: compound secondary keys like "10 Inch / Medium Firm"
    // _hasExtraCombos detects these; _extraDimName gives the correct dim name
    const _primName = primaryGroup?.name || product._primaryDimName || '';
    const _secName  = secondaryGroup?.name || product._secondaryDimName || '';
    let _extraDimNames = product._extraDimNames || (product._extraDimName ? [product._extraDimName] : []); // all extra dims
    // Auto-detect: if compound keys exist but _extraDimNames not stored (old cached products),
    // count extra parts from a sample key and use position-based eBay names
    if (_hasExtraCombos && _extraDimNames.length === 0) {
      const _sampleKey = Object.keys(comboAsin).find(k => k.includes(' / ')) || '';
      const _sampleAfterPipe = _sampleKey.split('|')[1] || '';
      const _extraCount = (_sampleAfterPipe.split(' / ').length - 1); // parts beyond secondary
      if (_extraCount > 0) {
        // Try to get names from product dimensions not already used as primary/secondary
        const _allProdDims = (product.variations || []).map(v => v.name);
        const _unusedDims = _allProdDims.filter(n => n !== _primName && n !== _secName);
        const _fallbackNames = ['Pattern', 'Configuration', 'Style2', 'Option', 'Variant'];
        for (let _ei = 0; _ei < _extraCount; _ei++) {
          _extraDimNames.push(_unusedDims[_ei] || _fallbackNames[_ei] || `Option${_ei+1}`);
        }
        console.log(`[buildVariants] auto-detected ${_extraCount} extra dims: ${_extraDimNames.join(', ')}`);
        // Write back so buildVariesBy also sees them
        product._extraDimNames = [..._extraDimNames];
      }
    }
    const _has3Dims = _hasExtraCombos && _extraDimNames.length > 0;

    for (const [key, asin] of Object.entries(comboAsin)) {
      if (_seen.has(key)) continue; _seen.add(key);
      const pipeIdx = key.indexOf('|');
      const pVal = key.slice(0, pipeIdx);
      const afterPipe = key.slice(pipeIdx + 1);
      if (!pVal) continue;

      // Split compound secondary "10 Inch / Medium Firm / Cooling" → sVal + extra parts
      // Supports 3, 4, 5+ dims: each " / " segment maps to next extra dim name
      const _allParts = afterPipe.split(' / ');
      const sVal = _allParts[0];
      const _extraVals = _allParts.slice(1); // ["Medium Firm"] or ["Medium Firm", "Cooling"] etc

      // Skip keys missing required extra dims — ensures ALL included variants are consistent N-dim
      // Old imported data may have mixed compound/plain keys; plain ones get skipped cleanly
      if (_has3Dims && _extraVals.length < _extraDimNames.length) continue;

      const price = comboPrices[key] || 0;
      // Per-ASIN shipping: use if captured, else 0 (handling already included via makeApplyMk)
      const _perAsinShip = product.comboShipping?.[key] ?? null;
      const ebayPrice = applyMk(price, _perAsinShip);
      const inStock = comboInStock[key] !== false && price > 0;
      if (_pValImgIdx[pVal] === undefined) _pValImgIdx[pVal] = _nextImgIdx++;

      // Build dims — 2+ dims depending on listing type
      const dims = { [_primName || primaryGroup?.name || 'Color']: pVal, [_secName || secondaryGroup?.name || 'Size']: sVal };
      for (let _di = 0; _di < _extraVals.length; _di++) {
        if (_extraVals[_di] && _extraDimNames[_di]) dims[_extraDimNames[_di]] = _extraVals[_di];
      }

      variants.push({
        sku:         mkSku([pVal, sVal, ..._extraVals].filter(Boolean)),
        dims,
        dimKey:      key,
        primaryVal:  pVal,
        secondaryVal: sVal,
        extraVal:    _extraVals[0] || '', // first extra dim value (for backwards compat)
        price:       (ebayPrice > 0 ? ebayPrice : 0).toFixed(2),
        qty:         inStock ? defaultQty : 0,
        image:       getImage(pVal, _pValImgIdx[pVal]),
        asin,        // expose ASIN so push imageUrls can use per-combo images
      });
    }
    // Also add combos not in comboAsin (existed on Amazon but weren't fetched/trimmed)
    // These get price=0 qty=0 — still need to be listed so the variant exists on eBay
    // but won't be purchasable
    for (const pv of primaryGroup.values) {
      for (const sv of secondaryGroup.values) {
        const plainKey = `${pv.value}|${sv.value}`;
        // Check if any compound key starting with this prefix already added
        if ([...variants].some(v => v.primaryVal === pv.value && v.secondaryVal?.startsWith(sv.value))) continue;
        // Not in comboAsin — skip entirely (combo doesn't exist on Amazon)
      }
    }
  } else if (primaryGroup && secondaryGroup) {
    // Standard 2-dim listing (Color × Size, Style × Color etc.)
    // Image indexed by primary value so all sizes of the same color share the same color photo
    let _vi = 0;
    for (const pv of primaryGroup.values) {
      const _pvImg = getImage(pv.value, _vi++); // one image per primary value
      for (const sv of secondaryGroup.values) {
        const inStock = isInStock(pv.value, sv.value);
        const _dk2 = `${pv.value}|${sv.value}`;
        const ebayPrice = applyMk(getAmazonPrice(pv.value, sv.value), product.comboShipping?.[_dk2] ?? null);
        variants.push({
          sku:          mkSku([pv.value, sv.value]),
          dims:         { [primaryGroup.name]: pv.value, [secondaryGroup.name]: sv.value },
          dimKey:       _dk2,
          primaryVal:   pv.value,
          secondaryVal: sv.value,
          price:        (ebayPrice > 0 ? ebayPrice : 0).toFixed(2),
          qty:          inStock ? defaultQty : 0,
          image:        _pvImg, // all sizes of same color share same image
        });
      }
    }
  } else if (primaryGroup) {
    // Single dimension
    for (let _pi = 0; _pi < primaryGroup.values.length; _pi++) {
      const pv = primaryGroup.values[_pi];
      const inStock = isInStock(pv.value, '');
      const _dk1 = `${pv.value}|`;
      const ebayPrice = applyMk(getAmazonPrice(pv.value, ''), product.comboShipping?.[_dk1] ?? null);
      variants.push({
        sku:          mkSku([pv.value]),
        dims:         { [primaryGroup.name]: pv.value },
        dimKey:       _dk1,
        primaryVal:   pv.value,
        secondaryVal: '',
        price:        (ebayPrice > 0 ? ebayPrice : 0).toFixed(2),
        qty:          inStock ? defaultQty : 0,
        image:        getImage(pv.value, _pi),
      });
    }
  }

  // eBay allows max 250 variants per listing
  // Sort: in-stock first, then OOS — so if we must cap, available variants are kept
  variants.sort((a, b) => {
    const aIn = a.qty > 0 ? 0 : 1;
    const bIn = b.qty > 0 ? 0 : 1;
    return aIn - bIn;
  });
  // FINAL DEDUP: mkSku only adds a hash suffix when the raw exceeds maxSuffix.
  // Two variants whose normalized values collide to the same short string both
  // return `prefix+raw` with no hash → identical SKUs → eBay rejects the entire
  // group with errorId 25724 "Duplicate SKUs found in the list". Walk the final
  // list and rename any collision by appending an index. Preserves the prefix
  // length budget by trimming the original SKU enough to fit the index suffix.
  const _seenSkus = new Set();
  let _renamed = 0;
  for (const v of variants) {
    if (!_seenSkus.has(v.sku)) {
      _seenSkus.add(v.sku);
      continue;
    }
    // Collision — append _2, _3, ... until unique. Trim original if needed
    // so the result still fits within 50 chars.
    const _SKU_MAX = 50;
    let _idx = 2;
    let _newSku;
    do {
      const _suffix = '_' + _idx;
      const _maxBase = _SKU_MAX - _suffix.length;
      const _base = v.sku.length > _maxBase ? v.sku.slice(0, _maxBase) : v.sku;
      _newSku = _base + _suffix;
      _idx++;
    } while (_seenSkus.has(_newSku) && _idx < 1000);
    console.warn(`[buildVariants] dedup collision: "${v.sku}" → "${_newSku}"`);
    v.sku = _newSku;
    _seenSkus.add(_newSku);
    _renamed++;
  }
  if (_renamed > 0) console.log(`[buildVariants] renamed ${_renamed} colliding SKU${_renamed>1?'s':''}`);
  return variants.slice(0, 100);
}

// Build the group variesBy spec — must list ALL values for each variation dimension
function buildVariesBy(product, primaryGroup, secondaryGroup, actualVariants) {
  // Normalize variation spec names to eBay-accepted names for Women's Clothing (11450)
  // eBay only accepts 'Color' and 'Size' as variation spec names for apparel
  function normSpecName(name) {
    if (!name) return name;
    if (/color|colour/i.test(name)) return 'Color';
    if (/^size$/i.test(name)) return 'Size';
    // Normalize Amazon internal dim keys → eBay-friendly aspect names
    const KEY_MAP = {
      item_package_quantity: 'Package Quantity',
      package_quantity:      'Package Quantity',
      unit_count_type:       'Unit Count',
      configuration_name:    'Configuration',
      length_name:           'Length',
      width_name:            'Width',
      height_name:           'Height',
    };
    const key = name.toLowerCase().replace(/\s+/g, '_');
    if (KEY_MAP[key]) return KEY_MAP[key];
    // All other dims keep their actual name
    return name;
  }
  // Include ALL actual variant values in specs — cap each value at 65 chars individually.
  // The old 65-char TOTAL limit was causing 25013 by truncating most colors from the spec.
  const _truncVal = v => String(v || '').slice(0, 65).trim();
  const _fitSpecValues = (values) => {
    const unique = [...new Set(values.map(v => String(v||'').trim()).filter(Boolean))];
    return unique.map(v => v.slice(0, 65).trim()).filter(Boolean);
  };
  // Build specs from ACTUAL variant values passed in (not full product color/size lists)
  // This prevents 25013 "specs don't match" when variants are capped to 100
  // and 25718 "values too long" because we only include values that exist in kept variants
  const specs = [];
  if (primaryGroup) {
    // Use only values that appear in the actual variants being pushed
    const primActual = actualVariants
      ? [...new Set(actualVariants.map(v => v.primaryVal).filter(Boolean))]
      : primaryGroup.values.map(v => v.value);
    const primVals = _fitSpecValues(primActual);
    if (primVals.length) specs.push({ name: normSpecName(primaryGroup.name), values: primVals });
  }
  if (secondaryGroup) {
    const secActual = actualVariants
      ? [...new Set(actualVariants.map(v => v.secondaryVal).filter(Boolean))]
      : secondaryGroup.values.map(v => v.value);
    const secVals = _fitSpecValues(secActual);
    if (secVals.length) specs.push({ name: normSpecName(secondaryGroup.name), values: secVals });
  }
  // Extra dimensions (3rd, 4th, 5th...) from _extraDimNames — N specs for N extra dims
  if (actualVariants && (product._extraDimNames?.length || product._extraDimName)) {
    const _eDimNames = product._extraDimNames || (product._extraDimName ? [product._extraDimName] : []);
    for (const _eDimName of _eDimNames) {
      const _eVals = [...new Set(actualVariants.map(v => v.dims?.[_eDimName]).filter(Boolean))];
      const _eSpecVals = _fitSpecValues(_eVals);
      if (_eSpecVals.length) specs.push({ name: normSpecName(_eDimName), values: _eSpecVals });
    }
  }
  // aspectsImageVariesBy: which dimension has per-variant images?
  // Check variationImages first (color swatch images), then check if variants have distinct images
  // Check both variationImages AND AliExpress per-variation .images for image presence
  const _aeVarImgsCheck = product.variations?.find(v => v.name === primaryGroup?.name)?.images || {};
  const _varImgValues = [
    ...Object.values(product.variationImages?.[primaryGroup?.name] || {}),
    ...Object.values(_aeVarImgsCheck),
  ].filter(v => Array.isArray(v) ? v.length > 0 : !!v);
  const _variantImages = (actualVariants || []).map(v => v.image).filter(Boolean);
  const _distinctImages = new Set(_variantImages).size;
  // Has per-variant images if: explicit variationImages map OR variants have 2+ distinct images
  const hasPerVariantImages = primaryGroup && (
    _varImgValues.length > 1 ||
    (_distinctImages > 1 && _variantImages.length > 0)
  );
  // For 3-dim listings: images vary by both primary (Fit_type) and secondary (Color)
  // eBay matches the right inventory item image when buyer selects a combination
  const _secondaryGroupForImg = secondaryGroup;
  const _extraDimsForImg = product._extraDimNames || [];
  let _variesByDims = [];
  if (hasPerVariantImages) {
    _variesByDims.push(normSpecName(primaryGroup.name));
    // Include secondary dim if it's a color/style (not size) — affects image selection
    if (_secondaryGroupForImg && !/(size|length|weight|width|volume|count|pack|qty)/i.test(_secondaryGroupForImg.name)) {
      _variesByDims.push(normSpecName(_secondaryGroupForImg.name));
    }
    // Include extra dims (e.g. Color in a 3-dim listing)
    for (const ed of _extraDimsForImg) {
      if (!/(size|length|weight|width|volume|count|pack|qty)/i.test(ed)) {
        _variesByDims.push(normSpecName(ed));
      }
    }
    _variesByDims = [...new Set(_variesByDims)];
  }
  return {
    aspectsImageVariesBy: _variesByDims,
    specifications: specs,
  };
}


// ── Category resolution: eBay taxonomy suggestion + comprehensive keyword fallback ──
async function resolveCategory(token, product) {
  // 1. Use scrape-time stored category if present
  if (product.ebayCategory && /^\d+$/.test(String(product.ebayCategory))) {
    return String(product.ebayCategory);
  }

  const title = product.title || '';
  const crumbs = (product.breadcrumbs || []).join(' ');
  const all = (title + ' ' + crumbs).toLowerCase();
  const T = title.toLowerCase();
  const A = all;

  // 2. Try eBay taxonomy suggestion API
  try {
    const EBAY_API = getEbayUrls().EBAY_API;
    const r = await fetch(
      `${EBAY_API}/commerce/taxonomy/v1/category_tree/0/get_category_suggestions?q=${encodeURIComponent(title.slice(0, 80))}`,
      { headers: { Authorization: `Bearer ${token}`, 'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US' } }
    );
    if (r.ok) {
      const d = await r.json();
      const first = d.categorySuggestions?.[0]?.category?.categoryId;
      if (first) { console.log(`[category] taxonomy API -> ${first}`); return first; }
    }
  } catch (e) { console.warn('[category] taxonomy API error:', e.message); }

  // 3. Comprehensive keyword fallback
  // Electronics
  if (/headphone|earphone|earbud|airpod/i.test(A)) return '112529';
  if (/bluetooth speaker|portable speaker/i.test(A)) return '14969';
  if (/laptop|notebook computer/i.test(A)) return '177';
  if (/tablet|ipad/i.test(A)) return '171485';
  if (/smart watch|smartwatch|fitness tracker/i.test(A)) return '178893';
  if (/phone case|iphone case|samsung case/i.test(A)) return '9394';
  if (/charger|charging cable|usb cable|power bank/i.test(A)) return '44956';
  if (/keyboard/i.test(A) && !/piano|music/i.test(A)) return '3676';
  if (/computer mouse|gaming mouse/i.test(A)) return '26252';
  if (/led strip|light strip|smart light|ring light|desk lamp|led light/i.test(A)) return '20697';
  // Home & Kitchen
  if (/coffee maker|coffee machine|espresso machine/i.test(A)) return '20668';
  if (/air fryer/i.test(A)) return '43083';
  if (/blender|nutribullet|smoothie maker/i.test(A)) return '14100';
  if (/instant pot|pressure cooker|slow cooker/i.test(A)) return '43083';
  if (/cookware|frying pan|skillet|wok|pot set/i.test(A)) return '20607';
  if (/vacuum cleaner|robot vacuum/i.test(A)) return '21581';
  if (/humidifier/i.test(A)) return '43569';
  if (/shower curtain/i.test(A)) return '20719';
  if (/towel/i.test(A) && /bath|hand|kitchen/i.test(A)) return '20716';
  if (/bedding|sheet set|duvet|comforter/i.test(A)) return '20444';
  if (/pillow/i.test(A) && !/travel pillow/i.test(A)) return '20695';
  if (/blanket|throw/i.test(A) && !/exercise/i.test(A)) return '16135';
  if (/rug|area rug|floor mat|door mat/i.test(A) && !/yoga/i.test(A)) return '20573';
  if (/wall art|canvas print|poster/i.test(A)) return '262148';
  // Sports & Fitness
  if (/yoga mat|exercise mat|gym mat|workout mat/i.test(A)) return '26350';
  if (/dumbbell|barbell|kettlebell/i.test(A)) return '15273';
  if (/resistance band|exercise band/i.test(A)) return '72937';
  if (/yoga|foam roller/i.test(A)) return '158938';
  if (/running shoe|athletic shoe|sneaker/i.test(A)) return '15709';
  if (/water bottle|sports bottle/i.test(A)) return '36913';
  if (/backpack/i.test(A)) return '169291';
  if (/bicycle|cycling/i.test(A)) return '177831';
  // Beauty & Personal Care
  if (/shampoo|conditioner|hair mask/i.test(A)) return '11854';
  if (/face wash|moisturizer|serum|sunscreen/i.test(A)) return '11863';
  if (/makeup|mascara|foundation|lipstick|concealer/i.test(A)) return '31786';
  if (/perfume|cologne|fragrance/i.test(A)) return '180345';
  if (/hair dryer|blow dryer|flat iron|curling iron/i.test(A)) return '26395';
  // Clothing — check gender context
  if (/women|ladies|female|womens/i.test(A)) {
    if (/legging|tight/i.test(T)) return '63867';
    if (/hoodie|sweatshirt|pullover/i.test(T)) return '63868';
    if (/dress/i.test(T)) return '63861';
    if (/bra|bralette/i.test(T)) return '11516';
    if (/pajama|sleepwear/i.test(T)) return '11533';
    if (/top|shirt|tee|blouse|tunic/i.test(T)) return '63862';
    if (/pant|jean|trouser|short|skirt/i.test(T)) return '63863';
    if (/jacket|coat|blazer/i.test(T)) return '63864';
    if (/swimsuit|bikini/i.test(T)) return '3003';
    if (/sock/i.test(T)) return '11530';
    return '11450'; // Women's Clothing
  }
  if (/\bmen\b|male|mens/i.test(A)) {
    if (/hoodie|sweatshirt/i.test(T)) return '57988';
    if (/shirt|tee|polo/i.test(T)) return '185100';
    if (/pant|jean|trouser|short/i.test(T)) return '57989';
    if (/sock/i.test(T)) return '11527';
    return '1059'; // Men's Clothing
  }
  if (/baby|infant|toddler/i.test(A)) return '182294';
  if (/kids|children/i.test(A)) return '171146';
  if (/legging|tight/i.test(T)) return '63867';
  if (/dress/i.test(T)) return '63861';
  if (/shirt|tee|blouse|top/i.test(T)) return '63862';
  if (/pant|jean|trouser/i.test(T)) return '11483';
  // Toys & Games
  if (/lego|building block/i.test(A)) return '183446';
  if (/board game|card game|puzzle/i.test(A) && !/jigsaw puzzle.*mat/i.test(A)) return '233';
  if (/action figure|doll|barbie/i.test(A)) return '246';
  // Pet Supplies
  if (/dog|puppy|canine/i.test(A)) return '20744';
  if (/cat|kitten|feline/i.test(A)) return '20741';
  // Health
  if (/vitamin|supplement|protein powder/i.test(A)) return '180959';
  if (/massage gun/i.test(A)) return '73836';
  // Default
  return '99'; // Everything Else (eBay catches all)
}
// ─── PUSH action (implementation below) ─────────────────────────────────────


// ─── REVISE action ─────────────────────────────────────────────────────────────


async function handleRevise(body, res) {
  // ── SPLIT REVISE — 3 sequential steps, each well under 300s ─────────────
  // Frontend calls step 1 → gets offerMap
  //                 step 2 → sends offerMap, gets asinPrices
  //                 step 3 → sends offerMap + asinPrices, eBay gets updated
  const step = parseInt(body.reviseStep) || 1;
  const EBAY_API = 'https://api.ebay.com';
  const auth = { Authorization: `Bearer ${body.access_token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };

  if (!body.access_token) return res.status(401).json({ error: 'No access token' });
  const ebaySku = (body.ebaySku || '').trim();
  if (!ebaySku) return res.status(400).json({ error: 'Missing ebaySku' });

  // ── STEP 1: Read all eBay offers ────────────────────────────────────────
  if (step === 1) {
    let listingId = body.ebayListingId || null;

    // ── Fast path: use cached offerIds if provided by client ─────────────────
    // offerIds are stable (don't change unless listing is re-pushed).
    // Client caches them and sends back — skips 30 individual GET /offer calls.
    const cachedOfferIds = body.cachedOfferIds || {}; // { sku: offerId }
    const hasCachedOffers = Object.keys(cachedOfferIds).length > 0;

    // ── 1. Get variant SKUs from inventory_item_group (1 API call) ────────────
    const _grp = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, { headers: auth })
      .then(r => r.json()).catch(() => ({}));
    let existingSkus = _grp.variantSKUs || [];
    if (!listingId && _grp.listing?.listingId) listingId = _grp.listing.listingId;

    // Fallback: scan offers by listingId if group has no SKUs
    if (existingSkus.length === 0 && listingId) {
      let _off = 0;
      while (existingSkus.length < 500) {
        const _ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=100&offset=${_off}`, { headers: auth })
          .then(r => r.json()).catch(() => ({}));
        for (const o of (_ol.offers || []))
          if (o.listing?.listingId === String(listingId) && !existingSkus.includes(o.sku))
            existingSkus.push(o.sku);
        if ((_ol.offers || []).length < 100) break;
        _off += 100;
      }
    }

    const offerMap = {};

    if (hasCachedOffers) {
      // ── Fast path: use cached offerIds + bulk_get_inventory_item for quantities ─
      // Instead of 30 GET /offer calls, just 2 bulk calls
      console.log(`[revise] step1: using ${Object.keys(cachedOfferIds).length} cached offerIds`);

      const skusToFetch = existingSkus.length > 0 ? existingSkus : Object.keys(cachedOfferIds);

      // Bulk GET inventory items for quantities (25 per call)
      const inventoryQtys = {}; // sku → availableQuantity
      for (let i = 0; i < skusToFetch.length; i += 25) {
        const batch = skusToFetch.slice(i, i + 25);
        const bgr = await fetch(`${EBAY_API}/sell/inventory/v1/bulk_get_inventory_item`, {
          method: 'POST', headers: auth,
          body: JSON.stringify({ requests: batch.map(s => ({ sku: s })) }),
        }).then(r => r.json()).catch(() => ({}));
        for (const resp of (bgr.responses || [])) {
          if (resp.sku) inventoryQtys[resp.sku] = resp.inventoryItem?.availability?.shipToLocationAvailability?.quantity ?? 0;
        }
        if (i + 25 < skusToFetch.length) await sleep(100);
      }

      // Build offerMap from cache + quantities
      for (const sku of skusToFetch) {
        const offerId = cachedOfferIds[sku];
        if (!offerId) continue;
        offerMap[sku] = {
          offerId,
          price:  0, // will be recalculated from Amazon price in step3
          qty:    inventoryQtys[sku] ?? 0,
          status: 'PUBLISHED',
        };
        if (!listingId) {
          // Try to get listingId from one offer (lazy)
          try {
            const _or = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerId}`, { headers: auth }).then(r => r.json()).catch(()=>({}));
            if (_or.listing?.listingId) listingId = _or.listing.listingId;
          } catch(e) {}
        }
      }
      console.log(`[revise] step1 (cached): ${Object.keys(offerMap).length} offers in ${skusToFetch.length} SKUs`);
    } else {
      // ── Slow path: read all offers individually (first revise, no cache yet) ──
      console.log(`[revise] step1: no cache — fetching ${existingSkus.length} offers individually`);
      for (let i = 0; i < existingSkus.length; i += 15) {
        const results = await Promise.all(
          existingSkus.slice(i, i + 15).map(s =>
            fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(s)}`, { headers: auth })
              .then(r => r.json()).catch(() => ({}))
          )
        );
        for (let j = 0; j < results.length; j++) {
          const offer = (results[j].offers || [])[0];
          if (offer && offer.status !== 'ENDED') {
            offerMap[existingSkus[i + j]] = {
              offerId: offer.offerId,
              price:   parseFloat(offer.pricingSummary?.price?.value || 0),
              qty:     offer.availableQuantity ?? 0,
              status:  offer.status,
            };
            if (!listingId && offer.listing?.listingId) listingId = offer.listing.listingId;
          }
        }
        if (i + 15 < existingSkus.length) await sleep(80);
      }
    }

    const endedCount = existingSkus.length - Object.keys(offerMap).length;
    console.log(`[revise] step1: ${Object.keys(offerMap).length}/${existingSkus.length} offers (${endedCount} ended)`);

    if (Object.keys(offerMap).length === 0)
      return res.status(503).json({ error: 'All offers ended — listing needs re-push.', skippable: true });

    // Return offerIds so client can cache them for next revise
    const offerIdsBySku = Object.fromEntries(Object.entries(offerMap).map(([sku, o]) => [sku, o.offerId]));
    return res.json({ success: true, step: 1, offerMap, listingId, skuCount: existingSkus.length, offerIdsBySku });
  }

  // ── STEP 2: Fetch Amazon prices for the locked ASINs ────────────────────
  if (step === 2) {
    if (!body.sourceUrl) return res.status(400).json({ error: 'Missing sourceUrl' });
    const offerMap  = body.offerMap  || {};
    const skuToAsin = body.skuToAsin || {};

    // Build locked ASIN list from skuToAsin + comboAsin fallback
    if (Object.keys(skuToAsin).length === 0 && body.comboAsin) {
      const _normalize = s => s.toUpperCase().replace(/[^A-Z0-9]/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '');
      const normToAsin = {};
      for (const [key, asin] of Object.entries(body.comboAsin)) {
        if (!asin) continue;
        const [pv, sv=''] = key.split('|');
        const n = sv ? _normalize(pv) + '_' + _normalize(sv) : _normalize(pv);
        normToAsin[n] = asin;
        normToAsin[_normalize(pv)] = normToAsin[_normalize(pv)] || asin;
      }
      for (const sku of Object.keys(offerMap)) {
        const m = sku.match(/^DS-\d+-[A-Z0-9]+-(\S+)$/);
        if (!m) continue;
        const sfx = m[1];
        if (normToAsin[sfx]) { skuToAsin[sku] = normToAsin[sfx]; continue; }
        for (const [n, asin] of Object.entries(normToAsin)) {
          if (sfx.startsWith(n) || n.startsWith(sfx.split('_').slice(0,2).join('_'))) {
            skuToAsin[sku] = asin; break;
          }
        }
      }
      console.log(`[revise] step2: built skuToAsin from comboAsin — ${Object.keys(skuToAsin).length} entries`);
    }

    // Unique ASINs to fetch
    const asinToSkus = {};
    for (const [sku, asin] of Object.entries(skuToAsin)) {
      if (offerMap[sku]) (asinToSkus[asin] = asinToSkus[asin] || []).push(sku);
    }
    const uniqueAsins = Object.keys(asinToSkus);
    console.log(`[revise] step2: fetching ${uniqueAsins.length} unique ASINs from Amazon`);

    const asinPrice   = {};
    const asinInStock = {};
    const asinImages  = {}; // asin → [imgUrl, ...]
    const _fetchStart = Date.now();
    const _ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131 Safari/537.36';

    // ── BULK STRATEGY: fetch parent page once → extract ALL variant prices ──────
    // The parent ASIN page embeds all variant prices in twisterData/colorToAsin JSON
    // One request instead of N individual ASIN fetches — avoids blocking pattern
    const _parentAsin = body.parentAsin || (uniqueAsins.length > 0 ? uniqueAsins[0] : null);
    const _parentUrl = body.sourceUrl || (_parentAsin ? `https://www.amazon.com/dp/${_parentAsin}?th=1` : null);
    let _bulkHit = 0;

    if (_parentUrl && uniqueAsins.length > 1) {
      // Random startup jitter — avoids burst patterns to Amazon
      await sleep(Math.random() * 2000);
      console.log(`[revise] step2: trying BULK parent fetch (${uniqueAsins.length} ASINs) — 1 request`);
      const ph = await fetchPage(_parentUrl, _ua, 1);
      if (ph && ph.length > 10000) {
        // Extract all variant prices from embedded JSON in parent page
        // Amazon embeds asin→price maps in multiple formats
        const _priceMap = {}; // asin → price

        // ── Pattern A: "priceAmount":XX.XX anywhere near an ASIN ──────────────
        // Broadest scan — catches most Amazon JSON embedding formats
        const _allPrices = [...ph.matchAll(/"priceAmount"\s*:\s*([\d.]+)/g)].map(m => parseFloat(m[1])).filter(p => p > 0 && p < 10000);
        const _allAsinsInPage = [...ph.matchAll(/"(?:ASIN|asin)"\s*:\s*"([A-Z0-9]{10})"/g)].map(m => m[1]);
        // Try to find JSON blobs containing both ASIN and priceAmount
        for (const m of ph.matchAll(/"([A-Z0-9]{10})"[^{}]{0,500}?"priceAmount"\s*:\s*([\d.]+)/g)) {
          const p = parseFloat(m[2]);
          if (p > 0 && p < 10000) _priceMap[m[1]] = p;
        }
        // Reverse: priceAmount then ASIN within 500 chars
        for (const m of ph.matchAll(/"priceAmount"\s*:\s*([\d.]+)[^{}]{0,500}?"([A-Z0-9]{10})"/g)) {
          const p = parseFloat(m[1]);
          if (p > 0 && p < 10000 && uniqueAsins.includes(m[2]) && !_priceMap[m[2]]) _priceMap[m[2]] = p;
        }

        // ── Pattern B: priceToPay JSON format ───────────────────────────────────
        for (const m of ph.matchAll(/"([A-Z0-9]{10})"[^{}]{0,800}?"priceToPay"\s*:\s*\{"amount"\s*:\s*([\d.]+)/g)) {
          const p = parseFloat(m[2]);
          if (p > 0 && p < 10000 && !_priceMap[m[1]]) _priceMap[m[1]] = p;
        }

        // ── Pattern C: buyingPrice or displayPrice ───────────────────────────────
        for (const m of ph.matchAll(/"([A-Z0-9]{10})"[^{}]{0,400}?"(?:buyingPrice|displayPrice|basisPrice|savings)"\s*:\s*\{"amount"\s*:\s*([\d.]+)/g)) {
          const p = parseFloat(m[2]);
          if (p > 0 && p < 10000 && !_priceMap[m[1]]) _priceMap[m[1]] = p;
        }

        // ── Pattern D: landedPrice or price in offers ────────────────────────────
        for (const m of ph.matchAll(/"([A-Z0-9]{10})"[^{}]{0,600}?"(?:landedPrice|listPrice|price)"\s*:\s*\{"amount"\s*:\s*([\d.]+)/g)) {
          const p = parseFloat(m[2]);
          if (p > 0 && p < 10000 && !_priceMap[m[1]]) _priceMap[m[1]] = p;
        }

        // ── Pattern E: variationPrices JSON blob ─────────────────────────────────
        const _varPriceM = ph.match(/"variationPrices"\s*:\s*(\{[\s\S]{0,20000}?\})\s*[,}]/);
        if (_varPriceM) {
          try {
            const _vp = JSON.parse(_varPriceM[1]);
            for (const [asin, pObj] of Object.entries(_vp)) {
              if (/^[A-Z0-9]{10}$/.test(asin)) {
                const p = parseFloat(
                  pObj?.price?.moneyValueOrRange?.value?.amount ||
                  pObj?.priceAmount || pObj?.price?.amount || 0
                );
                if (p > 0) _priceMap[asin] = p;
              }
            }
          } catch(e) {}
        }

        // ── Pattern F: twister inline prices ─────────────────────────────────────
        for (const m of ph.matchAll(/data-defaultAsin="([A-Z0-9]{10})"[^>]*>[\s\S]{0,2000}?"\$\s*([\d,.]+)"/g)) {
          const p = parseFloat(m[2].replace(/,/g,''));
          if (p > 0 && p < 10000 && !_priceMap[m[1]]) _priceMap[m[1]] = p;
        }

        // ── Pattern G: a-price-whole + a-price-fraction spans tied to ASIN ──────
        for (const m of ph.matchAll(/data-asin="([A-Z0-9]{10})"[\s\S]{0,2000}?a-price-whole[^>]*>(\d+)<[\s\S]{0,100}?a-price-fraction[^>]*>(\d+)</g)) {
          const p = parseFloat(m[2] + '.' + m[3]);
          if (p > 0 && p < 10000 && !_priceMap[m[1]]) _priceMap[m[1]] = p;
        }

        // ── Pattern H: JSON with "price":{"value":XX.XX} near ASIN ─────────────
        for (const m of ph.matchAll(/"([A-Z0-9]{10})"[\s\S]{0,400}?"price"\s*:\s*\{"value"\s*:\s*"?([\d.]+)"?/g)) {
          const p = parseFloat(m[2]);
          if (p > 0 && p < 10000 && uniqueAsins.includes(m[1]) && !_priceMap[m[1]]) _priceMap[m[1]] = p;
        }

        console.log(`[revise] step2 bulk extraction: found ${Object.keys(_priceMap).length} prices, page=${ph.length}b, priceAmounts=${_allPrices.length}, asinsInPage=${_allAsinsInPage.length}`);

        // Map extracted prices to our uniqueAsins
        for (const asin of uniqueAsins) {
          if (_priceMap[asin]) {
            asinPrice[asin]   = _priceMap[asin];
            asinInStock[asin] = true; // if price found = in stock
            _bulkHit++;
          }
        }

        // Also grab main product images from parent page
        const _mainImgs = extractAllImages(ph);
        if (_mainImgs.length && _parentAsin) asinImages[_parentAsin] = _mainImgs;

        console.log(`[revise] step2 bulk: found ${_bulkHit}/${uniqueAsins.length} prices from parent page`);
      } else {
        console.warn(`[revise] step2 bulk: parent page blocked — falling back to individual fetches`);
      }
    }

    // ── INDIVIDUAL FALLBACK: only fetch ASINs not covered by bulk ───────────────
    const _missing = uniqueAsins.filter(a => !asinPrice[a]);
    if (_missing.length > 0) {
      console.log(`[revise] step2: fetching ${_missing.length} remaining ASINs individually`);
    }

    for (let i = 0; i < _missing.length; i += 2) {
      if (Date.now() - _fetchStart > 220000) {
        console.warn(`[revise] step2: budget hit at ${i}/${_missing.length}`); break;
      }
      const batch = _missing.slice(i, i + 2);
      await Promise.all(batch.map(async (asin, bi) => {
        if (bi > 0) await sleep(600 + Math.random() * 600);
        const h = await fetchPage(`https://www.amazon.com/dp/${asin}?th=1&psc=1`, _ua, 1);
        if (!h) {
          // Blocked by Amazon — mark OOS (qty=0) per user preference
          asinInStock[asin] = false;
          console.log(`[revise] ${asin} blocked — marking qty=0`);
          return;
        }
        const price = extractPriceFromBuyBox(h);
        if (price > 0) {
          asinPrice[asin]   = price;
          asinInStock[asin] = !isAmazonOOS(h.match(/id="availability"[\s\S]{0,3000}/)?.[0] || '');
        } else {
          asinInStock[asin] = false;
        }
        const _imgs = extractAllImages(h);
        if (_imgs.length) asinImages[asin] = _imgs;
      }));
      const b0 = batch[0], b1 = batch[1];
      console.log(`[revise] step2: ${b0}→${asinPrice[b0]?'$'+asinPrice[b0]:'blocked'}${b1?' | '+b1+'→'+(asinPrice[b1]?'$'+asinPrice[b1]:'blocked'):''}`);
      // Longer randomized gap — datacenter IPs need time between requests to avoid pattern detection
      if (i + 2 < _missing.length) await sleep(3000 + Math.random() * 4000);
    }

    console.log(`[revise] step2: ${Object.values(asinPrice).length} prices, ${Object.values(asinImages).length} image sets fetched`);

    // ── Fast path: if browser sent prices (clientAsinData), skip separate step3 ─
    // Merge step2 + step3 into one call — saves a round trip
    const _hasBrowserPrices = (body.clientAsinData ? Object.keys(body.clientAsinData).length : 0) > 0 || Object.keys(asinPrice).length > 0;
    if (_hasBrowserPrices && body.mergeStep3) {
      console.log(`[revise] step2+3 merged — skipping separate step3 call`);
      // Inline step3 logic
      const _offerMap   = body.offerMap    || {};
      const _defaultQty = parseInt(body.quantity)    || 1;
      const _mkp        = parseFloat(body.markup)    || 23;
      const _handling   = parseFloat(body.handlingCost) || 2;
      const _fee        = 0.1335;
      const _applyMk    = cost => cost > 0
        ? Math.ceil(((cost + _handling) * (1 + _mkp / 100) / (1 - _fee) + 0.30) * 100) / 100
        : 0;

      const _updates = [], _priceChanges = [], _stockChanges = [];
      let _inStock = 0, _oos = 0;

      for (const [sku, offer] of Object.entries(_offerMap)) {
        if (!offer.offerId) continue;
        const asin = skuToAsin[sku];
        const amazonPrice = asin ? (asinPrice[asin] || 0) : 0;
        const isInStock = amazonPrice > 0 && asin && asinInStock[asin] !== false;
        const newPrice  = isInStock ? _applyMk(amazonPrice) : null;
        const newQty    = isInStock ? _defaultQty : 0;

        if (newQty > 0) _inStock++; else _oos++;
        const priceToSend = newPrice || 0;
        _updates.push({ offerId: offer.offerId, availableQuantity: newQty, price: { value: priceToSend.toFixed(2), currency: 'USD' } });
        if (newPrice && Math.abs(newPrice - offer.price) >= 0.01)
          _priceChanges.push(`${asin}: $${offer.price}→$${newPrice.toFixed(2)}`);
        if (newQty !== offer.qty)
          _stockChanges.push(`${sku.slice(-15)}: ${offer.qty}→${newQty}`);
      }

      let _updated = 0, _failed = 0;
      const _EBAY_API = getEbayUrls().EBAY_API;
      for (let i = 0; i < _updates.length; i += 25) {
        const _batch = _updates.slice(i, i + 25);
        const _br = await fetch(`${_EBAY_API}/sell/marketing/v1/bulk_update_price_quantity`, {
          method: 'POST', headers: auth, body: JSON.stringify({ requests: _batch }),
        }).then(r => r.json()).catch(() => ({}));
        for (const resp of (_br.responses || [])) {
          if ([200,201,204].includes(resp.statusCode)) _updated++;
          else _failed++;
        }
      }
      console.log(`[revise] step2+3: updated=${_updated} priceChanges=${_priceChanges.length} stockChanges=${_stockChanges.length}`);
      return res.json({
        success: true, step: '2+3', merged: true,
        asinPrice, asinInStock, skuToAsin,
        updated: _updated, failed: _failed,
        price: Object.values(asinPrice)[0] || 0,
        inStock: _inStock > 0,
        priceChanges: _priceChanges,
        stockChanges: _stockChanges,
      });
    }

    return res.json({ success: true, step: 2, asinPrice, asinInStock, asinImages, skuToAsin });
  }

  // ── STEP 3: Compare + update eBay offers ────────────────────────────────
  if (step === 3) {
    const offerMap    = body.offerMap    || {};
    const skuToAsin   = body.skuToAsin   || {};
    const asinPrice   = body.asinPrice   || {};
    const asinInStock = body.asinInStock || {};
    const asinImages  = body.asinImages  || {};
    let   listingId   = body.listingId   || body.ebayListingId || null;

    const defaultQty = parseInt(body.quantity)    || 1;
    const mkp        = parseFloat(body.markup)    || 23;
    const handling   = parseFloat(body.handlingCost) || 2;
    const fee = 0.1335;
    const applyMk = cost => cost > 0
      ? Math.ceil(((cost + handling) * (1 + mkp / 100) / (1 - fee) + 0.30) * 100) / 100
      : 0;

    // Build updates
    const updates = [];
    const priceChanges = [];
    const stockChanges = [];
    let inStock = 0, oos = 0;

    for (const [sku, offer] of Object.entries(offerMap)) {
      if (!offer.offerId) continue;
      const asin         = skuToAsin[sku];
      const amazonPrice  = asin ? (asinPrice[asin] || 0) : 0;
      const isInStock    = amazonPrice > 0 && asin && asinInStock[asin] !== false;
      const newEbayPrice = isInStock ? applyMk(amazonPrice) : null;
      const newQty       = isInStock ? defaultQty : 0;

      if (newQty > 0) inStock++; else oos++;

      const priceToSend = newEbayPrice || 0;
      updates.push({
        offerId:           offer.offerId,
        availableQuantity: newQty,
        price:             { value: priceToSend.toFixed(2), currency: 'USD' },
      });

      if (newEbayPrice && Math.abs(newEbayPrice - offer.price) >= 0.01)
        priceChanges.push(`${asin}: $${offer.price}→$${newEbayPrice.toFixed(2)}`);
      if (newQty !== offer.qty)
        stockChanges.push(`${sku.slice(-15)}: ${offer.qty}→${newQty}`);
    }

    console.log(`[revise] step3: ${updates.length} updates — ${inStock} in-stock, ${oos} OOS`);

    // Update inventory item imageUrls per SKU — GET first to preserve title/aspects, then PUT with new images
    const skusWithImages = Object.entries(skuToAsin).filter(([sku, asin]) => asinImages[asin]?.length && offerMap[sku]);
    if (skusWithImages.length > 0) {
      let imgUpdated = 0;
      for (let i = 0; i < skusWithImages.length; i += 8) {
        await Promise.all(skusWithImages.slice(i, i + 8).map(async ([_sku, _asin]) => {
          // GET current item to preserve all fields
          const gr = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(_sku)}`, { headers: auth })
            .then(r => r.json()).catch(() => null);
          if (!gr?.sku) return;
          // Merge: update only imageUrls and quantity, preserve everything else
          const updatedItem = {
            ...gr,
            product: {
              ...gr.product,
              imageUrls: asinImages[_asin].slice(0, 12),
            },
            availability: {
              ...gr.availability,
              shipToLocationAvailability: {
                ...(gr.availability?.shipToLocationAvailability || {}),
                quantity: offerMap[_sku]?.qty ?? gr.availability?.shipToLocationAvailability?.quantity ?? 1,
              },
            },
          };
          // Remove read-only fields
          delete updatedItem.locale; delete updatedItem.sku;
          const pr = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(_sku)}`, {
            method: 'PUT', headers: auth, body: JSON.stringify(updatedItem),
          });
          if (pr.ok || pr.status === 204) imgUpdated++;
        }));
        if (i + 8 < skusWithImages.length) await sleep(120);
      }
      console.log(`[revise] step3: updated images for ${imgUpdated}/${skusWithImages.length} SKUs`);
    }

    // Try bulk_update
    let updatedCount = 0;
    let need_recreate = false;

    for (let i = 0; i < updates.length; i += 25) {
      const _batch = updates.slice(i, i + 25);
      const r = await fetch(`${EBAY_API}/sell/inventory/v1/bulk_update_price_quantity`, {
        method: 'POST', headers: auth, body: JSON.stringify({ requests: _batch }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) {
        console.error(`[revise] step3 HTTP ${r.status}: ${JSON.stringify(d).slice(0, 200)}`);
        if ((d.errors || [])[0]?.errorId === 25001) { need_recreate = true; break; }
        continue;
      }
      for (const resp of (d.responses || [])) {
        if (!resp.errors?.length) updatedCount++;
        else console.warn(`[revise] offer error ${resp.offerId}: ${JSON.stringify(resp.errors[0]).slice(0,100)}`);
      }
      console.log(`[revise] step3 batch ${Math.floor(i/25)+1}: ${(d.responses||[]).filter(r=>!r.errors?.length).length}/${_batch.length}`);
    }

    // Fallback: 25001 — bulk_update failed. Use PUT /offer/{offerId} per-offer
    // Notes:
    //   - DELETE /offer only works on UNPUBLISHED offers — cannot delete ENDED or PUBLISHED
    //   - PUT /offer/{offerId} works on any state (PUBLISHED, ENDED, UNPUBLISHED)
    //   - For PUBLISHED offers: PUT updates in-place, no republish needed
    //   - For ENDED offers: PUT + then publish via group
    //   - pricingSummary.price (not price) is the correct field for individual offer PUT
    if (need_recreate) {
      const skuList = Object.keys(offerMap);
      console.log(`[revise] step3: 25001 fallback — PUT /offer/{id} for ${skuList.length} offers`);

      const _cat  = await resolveCategory(body.access_token, { title: body.fallbackTitle || '', sourceUrl: body.sourceUrl || '' }).catch(() => '11450');
      const _pols = await resolvePolicies(body.access_token).catch(() => ({}));
      const _loc  = await ensureLocation(auth, false).catch(() => 'MainWarehouse');

      // Step A: GET each offer to get its full current body, update price, PUT back
      // MUST use the existing offer body — eBay rejects PUT with a from-scratch body (25709)
      let putOk = 0, putFail = 0;

      for (let i = 0; i < skuList.length; i += 10) {
        await Promise.all(skuList.slice(i, i+10).map(async _sku => {
          const offer = offerMap[_sku];
          if (!offer?.offerId) return;
          const upd = updates.find(u => u.offerId === offer.offerId);
          const newQty = upd?.availableQuantity ?? 0;

          // Step 1: GET the current full offer body
          const gr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${encodeURIComponent(offer.offerId)}`, { headers: auth })
            .then(r => r.json()).catch(() => null);
          if (!gr?.offerId) { putFail++; return; }

          // Step 2: Update price in the fetched body
          // upd contains the price calculated from Amazon data (asinPrice → applyMarkup)
          // offer.price is 0 when using cached offerIds — always prefer upd price
          const newPrice = upd?.price?.value || '0';
          const offerBody = {
            ...gr,
            pricingSummary: { price: { value: newPrice, currency: 'USD' } },
          };
          // Remove read-only fields that can't be in PUT body
          delete offerBody.offerId; delete offerBody.offerState; delete offerBody.status;
          delete offerBody.listing; delete offerBody.marketplaceId;

          // Step 3: PUT back the full body with updated price
          // Content-Language header is required for PUT /offer/{offerId}
          const pr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${encodeURIComponent(offer.offerId)}`, {
            method: 'PUT',
            headers: { ...auth, 'Content-Language': 'en-US' },
            body: JSON.stringify(offerBody),
          });
          const pd = await pr.json().catch(() => ({}));
          if (pr.ok || pr.status === 204) {
            putOk++;
          } else {
            putFail++;
            console.warn(`[revise] PUT offer ${offer.offerId} HTTP ${pr.status}: ${JSON.stringify(pd).slice(0,150)}`);
          }

          // Update inventory item qty — GET first to preserve existing product data (images, aspects etc)
          const existingItem = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(_sku)}`, { headers: auth })
            .then(r => r.json()).catch(() => null);
          if (existingItem?.sku) {
            const updatedItem = {
              ...existingItem,
              availability: {
                ...(existingItem.availability || {}),
                shipToLocationAvailability: { quantity: newQty },
              },
            };
            delete updatedItem.locale; delete updatedItem.sku;
            await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(_sku)}`, {
              method: 'PUT', headers: auth, body: JSON.stringify(updatedItem),
            }).catch(() => {});
          }
        }));
        if (i + 10 < skuList.length) await sleep(150);
      }
      console.log(`[revise] PUT offers: ${putOk} ok, ${putFail} failed`);

      // Step B: if any offers are ENDED (PUT worked), republish via group
      // Re-GET offers to find new state
      const newOfferIds = {};
      for (let i = 0; i < skuList.length; i += 15) {
        const results = await Promise.all(
          skuList.slice(i, i+15).map(s =>
            fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(s)}`, { headers: auth })
              .then(r => r.json()).catch(() => ({}))
          )
        );
        for (let j = 0; j < results.length; j++) {
          const o = (results[j].offers || [])[0];
          if (o?.offerId) newOfferIds[skuList[i+j]] = { offerId: o.offerId, status: o.status };
        }
        if (i + 15 < skuList.length) await sleep(80);
      }

      const hasEnded = Object.values(newOfferIds).some(o => o.status === 'ENDED' || o.status === 'UNPUBLISHED');
      if (hasEnded) {
        console.log(`[revise] some offers ENDED/UNPUBLISHED — republishing group`);
        const pubR = await fetch(`${EBAY_API}/sell/inventory/v1/publish_by_inventory_item_group`, {
          method: 'POST', headers: auth,
          body: JSON.stringify({ inventoryItemGroupKey: ebaySku, marketplaceId: 'EBAY_US' }),
        });
        const pubD = await pubR.json().catch(() => ({}));
        if (pubD.listingId) { listingId = pubD.listingId; console.log(`[revise] republished: ${pubD.listingId}`); }
        else console.warn(`[revise] republish result: ${JSON.stringify(pubD).slice(0,200)}`);
      }

      // Step C: bulk_update with refreshed offer IDs
      const newUpdates = updates.map(upd => {
        const _sku = skuList.find(s => offerMap[s]?.offerId === upd.offerId);
        const newId = _sku ? newOfferIds[_sku]?.offerId : null;
        return newId ? { ...upd, offerId: newId } : null;
      }).filter(Boolean);

      for (let i = 0; i < newUpdates.length; i += 25) {
        const r2 = await fetch(`${EBAY_API}/sell/inventory/v1/bulk_update_price_quantity`, {
          method: 'POST', headers: auth, body: JSON.stringify({ requests: newUpdates.slice(i, i+25) }),
        });
        const d2 = await r2.json().catch(() => ({}));
        if (!r2.ok) { console.error(`[revise] final bulk HTTP ${r2.status}: ${JSON.stringify(d2).slice(0,200)}`); continue; }
        for (const resp of (d2.responses || []))
          if (!resp.errors?.length) updatedCount++;
          else console.warn(`[revise] final bulk error: ${JSON.stringify(resp.errors[0]).slice(0,100)}`);
      }
      console.log(`[revise] 25001 fallback done: ${updatedCount}/${newUpdates.length} updated`);
    }

    console.log(`[revise] DONE — updated=${updatedCount}/${updates.length} inStock=${inStock} OOS=${oos}`);

    return res.json({
      success: true,
      step: 3,
      sku: ebaySku,
      listingId,
      variantsCreated: updatedCount,
      freshScrape: true,
      type: 'inplace',
      inStock: inStock > 0,
      updatedVariants: updatedCount,
      priceChanges,
      stockChanges,
      newProduct: {
        comboAsin:  body.comboAsin,
        inStock:    inStock > 0,
      },
    });
  }

  return res.status(400).json({ error: `Unknown reviseStep: ${step}` });
}



async function handlePush({ body, res, resolvePolicies, sanitizeTitle, ensureLocation, buildOffer, sleep, getEbayUrls }) {
  const EBAY_API = getEbayUrls().EBAY_API; // production only
  const auth     = { Authorization: `Bearer ${body.access_token}`, 'Content-Type': 'application/json', 'Content-Language': 'en-US', 'Accept-Language': 'en-US' };
  const { access_token, product, fulfillmentPolicyId, paymentPolicyId, returnPolicyId } = body;
  if (!access_token || !product) return res.status(400).json({ error: 'Missing access_token or product' });

  // Guards
  if (!product.images?.length)
    return res.status(400).json({ error: 'No images — re-import from the Import tab first.' });
  if (!product.hasVariations && !product.price && !product.cost && !product.myPrice)
    return res.status(400).json({ error: 'No price — re-import from the Import tab first.' });

  // ── ZERO-VARIANTS GUARD (first-time push) ──────────────────────────────────
  // If the scraper's HARD CAP filter dropped every variant — every combo was
  // either OOS, missing a price, or had no real ASIN — there's nothing to
  // list. Refuse the push outright. This is the import-time path. (Sync of an
  // existing listing handles this differently in smartSync — it keeps the
  // listing alive and marks all variants OOS.)
  if (product._zeroVariantsAfterCap) {
    console.warn(`[push] BLOCKED: scraper trim left 0 real variants for "${(product.title||'').slice(0,60)}" — refusing first-time push`);
    return res.status(400).json({
      error: 'Cannot push: this Amazon product has no in-stock priced variants right now. Every color/size is either out of stock or missing a price. Wait for restock or pick a different product.',
      code: 'ZERO_VARIANTS_AFTER_CAP'
    });
  }

  // ── AMAZON $0 → FORCE QTY 0 (NON-NEGOTIABLE) ───────────────────────────────
  // Hard rule: if Amazon's scraped cost is 0, the listing publishes at qty=0
  // and stays unbuyable until the worker revises it back with a real price.
  // Applies to single-variant AND multi-variation listings, no exceptions.
  // No "force push" override, no manual cost fallback — Amazon $0 is the
  // signal that we don't know the real cost right now, and unknown cost means
  // unbuyable. The worker rotation will fix the qty automatically when
  // Amazon's price comes back on the next revise pass.
  //
  // Mechanism (rather than refusing):
  //   - Simple/single-variant: set product.inStock = false → simpleQty becomes 0
  //     downstream. Also bump product.price to a $9.99 placeholder so the
  //     markup math and eBay's $0.99 minimum-price requirement are satisfied.
  //     Since qty=0, no buyer can ever transact at the placeholder price.
  //   - Multi-variant: for every comboPrices entry with cost <= 0, set
  //     comboInStock[key] = false (downstream isInStock() returns false → qty=0
  //     for that variant) and bump comboPrices[key] to $9.99 placeholder for
  //     the same reason. Variants with real Amazon prices are untouched.
  {
    const _src = product._source || 'amazon';
    if (_src === 'amazon') {
      const PLACEHOLDER = 9.99; // arbitrary > $0.99 (eBay min); never sold because qty=0

      // Simple/single-variant case
      const _scrapedPrice = parseFloat(product.price) || 0;
      if (_scrapedPrice <= 0) {
        console.warn(`[push] AMAZON $0 → forcing OOS for "${(product.title||'').slice(0,60)}" (was product.price=${product.price})`);
        product.inStock = false;
        product.price = PLACEHOLDER;
      }

      // Multi-variant case: zero out any combo whose Amazon cost is 0
      if (product.comboPrices && typeof product.comboPrices === 'object') {
        if (!product.comboInStock || typeof product.comboInStock !== 'object') product.comboInStock = {};
        let _zeroed = 0;
        for (const [key, rawPrice] of Object.entries(product.comboPrices)) {
          const p = parseFloat(rawPrice) || 0;
          if (p <= 0) {
            product.comboInStock[key] = false;
            product.comboPrices[key]  = PLACEHOLDER;
            _zeroed++;
          }
        }
        if (_zeroed > 0) {
          console.warn(`[push] AMAZON $0 variants → forced ${_zeroed} variant(s) to OOS for "${(product.title||'').slice(0,60)}"`);
        }
      }
    }
  }

  // ── SINGLE-COMBO COLLAPSE ──────────────────────────────────────────────────
  // Some products come out of the scraper flagged as multi-variation (with
  // dim names like "Color"/"Size" populated) but with zero or one real combo.
  // Examples: Amazon products that have a variation picker UI on the page but
  // only one actual SKU; AliExpress DOM parses where combo synthesis produced
  // a single (pVal, sVal) pair; Webstaurant items with a one-entry variation
  // family. eBay's Inventory API rejects multi-variation listings with <2
  // variants outright, so the push silently fails.
  //
  // Fix: count real, priced combos. If <2, force single-variant mode by
  // wiping all the multi-variation fields. The downstream payload builder
  // sees a clean single-variant product and pushes it as a regular fixed-
  // price listing. This applies to all sources (Amazon/AE/WS) so any future
  // scraper edge case producing the same shape gets handled too.
  if (product.hasVariations) {
    const _combos = product.comboPrices || {};
    const _inStock = product.comboInStock || {};
    const _realCombos = Object.keys(_combos).filter(k => {
      const p = parseFloat(_combos[k]) || 0;
      if (p <= 0) return false;
      if (_inStock[k] === false) return false;
      return true;
    });
    if (_realCombos.length < 2) {
      console.log(`[push] collapsing to single-variant: hasVariations=true but only ${_realCombos.length} real combo(s) — dims were ${product._primaryDimName || 'none'}/${product._secondaryDimName || 'none'}`);
      // Promote the single combo's price to top-level if base price is missing
      if (!product.price && _realCombos.length === 1) {
        product.price = parseFloat(_combos[_realCombos[0]]) || product.price || 0;
      }
      product.hasVariations    = false;
      product.variations       = [];
      product.variationImages  = {};
      product.comboPrices      = {};
      product.comboInStock     = {};
      product.comboAsin        = {};
      product.sizePrices       = {};
      product._primaryDimName  = null;
      product._secondaryDimName = null;
      product._extraDimNames   = [];
      product._isFullyMultiAsin = false;
    }
  }

  // ── HARD COST FLOOR ────────────────────────────────────────────────────────
  // After the collapse above, a product may end up as single-variant with no
  // valid cost (e.g. Amazon scraper got price=0 because the item was OOS at
  // scrape time, or the collapsed combo had no price). Pushing a $0 product
  // to eBay is dangerous — buyers can grab free listings before we notice.
  // Refuse the push here with a clear error rather than letting the price
  // math silently produce $0.00 downstream.
  if (!product.hasVariations) {
    const _baseCost = parseFloat(product.price || product.cost || product.myPrice || 0) || 0;
    if (_baseCost <= 0) {
      console.warn(`[push] BLOCKED: "${(product.title||'').slice(0,60)}" has no valid base cost (price=${product.price}, cost=${product.cost}, myPrice=${product.myPrice}) — refusing to push at $0`);
      return res.status(400).json({
        error: 'Cannot push: no valid price detected for this product. Re-import from the Import tab, or check the source URL — the item may be out of stock or unavailable.',
        code: 'NO_PRICE'
      });
    }
  }

  // ── PRE-PUSH CLEANUP: delete any existing inventory for this SKU ────────────
  // Instead of returning early on duplicate, wipe clean and re-push fresh.
  // This ensures prices/images/variants are always current on re-push.
  if (body.existingEbaySku) {
    const normExisting = body.existingEbaySku.trim().replace(/\s+/g,'');
    console.log(`[push] pre-push cleanup: checking for existing inventory under ${normExisting.slice(0,35)}`);
    try {
      // Get group to find all variant SKUs
      const grpR = await fetch(
        `${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(normExisting)}`,
        { headers: auth }
      ).catch(() => null);
      if (grpR?.ok) {
        const grpData = await grpR.json().catch(() => ({}));
        const oldSkus = grpData.variantSKUs || [];
        // Delete all variant inventory items — but NOT the group itself
        // Deleting the group breaks the groupSku → listingId mapping → eBay creates NEW listing
        // Keeping the group means publish_by_inventory_item_group updates the SAME listing
        for (let _di = 0; _di < oldSkus.length; _di += 20) {
          await Promise.all(oldSkus.slice(_di, _di + 20).map(async sku => {
            await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`,
              { method: 'DELETE', headers: auth }).catch(() => {});
          }));
        }
        console.log(`[push] pre-push cleanup: deleted ${oldSkus.length} variant items (group kept → same listing ID preserved)`);
      }
    } catch(e) { console.warn('[push] pre-push cleanup error:', e.message); }
  }

  const markupPct      = parseFloat(body.markup  ?? 0);
  const handling       = parseFloat(body.handlingCost ?? 2);
  const defaultQty     = parseInt(body.quantity || product.quantity) || 1;
  const amazonShipping = parseFloat(product.shippingCost || 0); // paid shipping on Amazon
  // Price formula: (amazonCost + amazonShipping + handling) × markup ÷ (1−fee) + $0.30
  const applyMk = (cost, shipOverride) => {
    const c = parseFloat(cost) || 0;
    if (c <= 0) return 0;
    const ship = shipOverride !== undefined ? shipOverride : amazonShipping;
    return Math.max(
      Math.ceil(((c + ship + handling) * (1 + markupPct / 100) / (1 - 0.1335) + 0.30) * 100) / 100, 0
    );
  };

  console.log(`[push] "${product.title?.slice(0,60)}" hasVar=${product.hasVariations} imgs=${product.images?.length} markup=${markupPct}%`);

  // ── Sanitize images: deduplicate, cap to 12, strip CDN junk ─────────────────
  {
    const seen = new Set();
    product.images = (product.images || [])
      .filter(u => {
        if (!u || typeof u !== 'string' || !u.startsWith('http')) return false;
        if (seen.has(u)) return false;
        seen.add(u);
        return true;
      })
      .slice(0, 12);
    // Hydrate variationImages from variations[].values[].image if missing
    if (!product.variationImages) product.variationImages = {};
    for (const vg of (product.variations || [])) {
      if (!product.variationImages[vg.name]) product.variationImages[vg.name] = {};
      for (const vv of (vg.values || [])) {
        if (vv.image && !product.variationImages[vg.name][vv.value]) {
          product.variationImages[vg.name][vv.value] = vv.image;
        }
      }
    }
    console.log(`[push] sanitized: ${product.images.length} imgs, varImgDims=${Object.keys(product.variationImages).join(',') || 'none'}`);
  }

  // ── Sanitize description: strip garbage phrases ──────────────────────────────
  if (product.description) {
    product.description = product.description
      .replace(/<li[^>]*>[^<]*Image\s*(Unavailable|not\s*available)[^<]*<\/li>/gi, '')
      .replace(/<li[^>]*>[^<]*Select\s*your\s*preferred[^<]*<\/li>/gi, '')
      .replace(/<li[^>]*>[^<]*(Clothing,\s*Shoes|Tops,\s*Tees)[^<]*<\/li>/gi, '')
      .replace(/<li[^>]*>([^<]{0,15})<\/li>/gi, '') // strip very short li items
      .replace(/<ul>\s*<\/ul>/gi, '')
      .replace(/\s{2,}/g, ' ')
      .trim();
  }
  if (product.bullets?.length) {
    const GARBAGE = [/image\s*(unavailable|not\s*available)/i, /select\s*your\s*preferred/i,
      /clothing,\s*shoes/i, /tops,\s*tees/i, /›/, /^\s*[\d.%]+\s*$/, /ships\s*from/i];
    product.bullets = product.bullets.filter(b => !GARBAGE.some(p => p.test(b)));
  }

  // Policies
  let policies;
  try { policies = await resolvePolicies(access_token, { fulfillmentPolicyId, paymentPolicyId, returnPolicyId }); }
  catch(e) { return res.status(400).json({ error: e.message }); }

   // Category detection: use stored ebayCategory from scrape, fall back to keyword map
   let categoryId = await resolveCategory(access_token, product);
   console.log(`[push] category: ${categoryId} for "${product.title?.slice(0,40)}"`);
   const listingTitle = sanitizeTitle(neutralizeTitle(product.ebayTitle || product.title || 'Product', product));


  // Description
  const ebayDescription = buildDescription(listingTitle, product.bullets || [], product.descriptionPara || '', product.aspects || {})
    || product.description || listingTitle;

  // Base aspects (strip Color/Size — variants carry their own)
  const aspects = { ...(product.aspects || {}) };
  // Remove all variation dimension names from base aspects
  delete aspects['Color']; delete aspects['color'];
  delete aspects['Size'];  delete aspects['size'];
  if (product._primaryDimName)   { delete aspects[product._primaryDimName]; delete aspects[product._primaryDimName.toLowerCase()]; }
  if (product._secondaryDimName) { delete aspects[product._secondaryDimName]; delete aspects[product._secondaryDimName.toLowerCase()]; }

  // Fill required item specifics from product data
  {
    const _t = (product.title || '').toLowerCase();
    const _c = (product.breadcrumbs || []).join(' ').toLowerCase();
    const _isCloth = /shirt|pant|dress|top|legging|hoodie|sweat|jacket|coat|bra|sock|jean|blouse|vest|skirt|suit|scarf|hat|cap|beanie|glove|boot|shoe|sneaker|sandal|\bshort\b|underwear|thong|brief|lingerie|swimsuit|bikini|activewear|athletic.*wear/i.test(_t);
    const _isMen = /\bmen\b|\bmale\b/i.test(_t + ' ' + _c);
    const _isKid = /\bkid\b|\bchild\b|\btoddler\b/i.test(_t + ' ' + _c);
    if (!aspects['Brand']) aspects['Brand'] = ['Unbranded'];
    if (!aspects['Type'])  aspects['Type']  = ['Other'];
    if (_isCloth) {
      if (!aspects['Department'])  aspects['Department']  = [_isKid ? 'Kids/Boys' : (_isMen ? 'Men' : 'Women')];
      if (!aspects['Size Type'])   aspects['Size Type']   = ['Regular'];
      if (!aspects['Material']) {
        // Extract material from title/breadcrumbs
        const _mat = (_t + ' ' + _c).toLowerCase();
        if (/stainless.steel|steel/i.test(_mat)) aspects['Material'] = ['Stainless Steel'];
        else if (/aluminum|aluminium/i.test(_mat)) aspects['Material'] = ['Aluminum'];
        else if (/plastic|acrylic/i.test(_mat)) aspects['Material'] = ['Plastic'];
        else if (/cotton/i.test(_mat)) aspects['Material'] = ['Cotton'];
        else if (/polyester/i.test(_mat)) aspects['Material'] = ['Polyester'];
        else if (/nylon/i.test(_mat)) aspects['Material'] = ['Nylon'];
        else if (/spandex|lycra/i.test(_mat)) aspects['Material'] = ['Spandex'];
        else if (/wool/i.test(_mat)) aspects['Material'] = ['Wool'];
        else if (/leather/i.test(_mat)) aspects['Material'] = ['Leather'];
        else if (/silicone/i.test(_mat)) aspects['Material'] = ['Silicone'];
        else if (/rubber/i.test(_mat)) aspects['Material'] = ['Rubber'];
        else if (/wood|bamboo/i.test(_mat)) aspects['Material'] = ['Wood'];
        else if (/glass/i.test(_mat)) aspects['Material'] = ['Glass'];
        else if (/canvas/i.test(_mat)) aspects['Material'] = ['Canvas'];
        else if (/foam/i.test(_mat)) aspects['Material'] = ['Foam'];
        else if (/mesh/i.test(_mat)) aspects['Material'] = ['Mesh'];
        else if (/vinyl|pvc/i.test(_mat)) aspects['Material'] = ['Vinyl'];
        else aspects['Material'] = ['Synthetic'];
      }
      if (!aspects['Occasion'])    aspects['Occasion']    = ['Casual'];
      if (!aspects['Pattern'])     aspects['Pattern']     = ['Solid'];
      if (!aspects['Style']) {
        if (/bra\b|bralette|wireless.*bra|sports.*bra|strapless/i.test(_t)) aspects['Style'] = ['Wireless'];
        else if (/brief|boyshort|thong|panty|panties|bikini.*bottom/i.test(_t)) aspects['Style'] = ['Brief'];
        else if (/boxer|trunk/i.test(_t)) aspects['Style'] = ['Boxer'];
        else if (/bikini|swimsuit|one.piece/i.test(_t)) aspects['Style'] = ['Bikini'];
        else aspects['Style'] = ['Other'];
      }
      if (!aspects['Bra Size']) {
        if (/bra\b|bralette|wireless.*bra|sports.*bra/i.test(_t)) aspects['Bra Size'] = ['M'];
      }
    }
    // Furniture/sets
    const _isFurn = /dining.*set|bedroom.*set|sofa.*set|sectional|furniture.*set|piece.*set/i.test(_t);
    if (_isFurn) {
      if (!aspects['Set Includes']) {
        const pm = _t.match(/(\d+)\s*(?:piece|pc)/i);
        const n = pm ? parseInt(pm[1]) : 1;
        if (/dining/i.test(_t)) aspects['Set Includes'] = [`${Math.max(n-1,1)} Chair${n>2?'s':''}, Table`];
        else if (/bedroom/i.test(_t)) aspects['Set Includes'] = ['Bed Frame, Nightstand, Dresser'];
        else if (/sofa|sectional/i.test(_t)) aspects['Set Includes'] = ['Sofa'];
        else aspects['Set Includes'] = [`${n} Piece Set`];
      }
      if (!aspects['Number of Items in Set']) {
        const pm2 = _t.match(/(\d+)\s*(?:piece|pc|pack)/i);
        aspects['Number of Items in Set'] = [pm2 ? pm2[1] : '1'];
      }
      if (!aspects['Number of Pieces']) {
        const pm3 = _t.match(/(\d+)\s*(?:piece|pc)/i);
        if (pm3) aspects['Number of Pieces'] = [pm3[1]];
      }
      if (!aspects['Room']) {
        if (/dining/i.test(_t)) aspects['Room'] = ['Dining Room'];
        else if (/bedroom/i.test(_t)) aspects['Room'] = ['Bedroom'];
        else if (/living/i.test(_t)) aspects['Room'] = ['Living Room'];
        else if (/outdoor|patio/i.test(_t)) aspects['Room'] = ['Patio/Garden'];
        else if (/office/i.test(_t)) aspects['Room'] = ['Office'];
        else aspects['Room'] = ['Any Room'];
      }
      if (!aspects['Assembly Required']) aspects['Assembly Required'] = ['Yes'];
    }
    // Mattress / bedding specifics
    const _isMattress = /mattress|mattresses/i.test(_t);
    if (_isMattress) {
      if (!aspects['Firmness']) {
        if (/firm(?!ness)/i.test(_t)) aspects['Firmness'] = ['Firm'];
        else if (/soft/i.test(_t)) aspects['Firmness'] = ['Soft'];
        else if (/plush/i.test(_t)) aspects['Firmness'] = ['Medium Plush'];
        else if (/medium/i.test(_t)) aspects['Firmness'] = ['Medium'];
        else aspects['Firmness'] = ['Medium'];
      }
      if (!aspects['Type']) {
        if (/memory.foam/i.test(_t)) aspects['Type'] = ['Memory Foam'];
        else if (/innerspring|spring/i.test(_t)) aspects['Type'] = ['Innerspring'];
        else if (/hybrid/i.test(_t)) aspects['Type'] = ['Hybrid'];
        else if (/latex/i.test(_t)) aspects['Type'] = ['Latex'];
        else aspects['Type'] = ['Memory Foam'];
      }
      if (!aspects['Thickness']) {
        const thickM = _t.match(/(\d{1,2})\s*(?:inch|in\.?|")/i);
        if (thickM) aspects['Thickness'] = [thickM[1] + ' in'];
      }
      if (!aspects['Features']) {
        aspects['Features'] = ['CertiPUR-US Certified'];
      }
    }
    // Weight vest / fitness
    if (/weighted.vest|weight.vest/i.test(_t)) {
      if (!aspects['Weight']) {
        const wM = _t.match(/(\d+)\s*(?:lb|pound|kg)/i);
        if (wM) aspects['Weight'] = [wM[1] + (/ kg/i.test(_t) ? ' kg' : ' lb')];
      }
    }
    // Department for non-clothing items (bags, accessories, etc.)
    if (!aspects['Department'] && !_isCloth) {
      if (/\bwomen\b|\bfemale\b|\bladies\b/i.test(_t + ' ' + _c)) aspects['Department'] = ['Women'];
      else if (/\bmen\b|\bmale\b/i.test(_t + ' ' + _c)) aspects['Department'] = ['Men'];
    }
    // Dimension aspects for storage/organizer products
    if (!aspects['Product']) {
      // Only add if category might need it
      if (/organizer|storage|shelf|rack|caddy|holder/i.test(_t)) aspects['Product'] = [(_t).slice(0,65)];
    // Dimension aspects — extract from title or use category-appropriate defaults
    // Many kitchen/storage/home categories require these
    const _extractDim = (title, units=['inch','in\.?','"','cm']) => {
      const pat = new RegExp('(\\d+(?:\\.\\d+)?)\\s*(?:x\\s*\\d+(?:\\.\\d+)?\\s*x\\s*\\d+(?:\\.\\d+)?\\s*)?(?:' + units.join('|') + ')', 'i');
      const m = title.match(pat);
      return m ? m[1] + ' in' : null;
    };
    if (!aspects['Item Width'] && !aspects['Item Length'] && !aspects['Item Height']) {
      // Only auto-add for categories that commonly require dimensions
      if (/rack|drying|organizer|shelf|basket|bin|tray|caddy|drawer|cabinet|stand|holder|board|mat/i.test(_t)) {
        const dim = _extractDim(_t);
        if (!aspects['Item Width'])  aspects['Item Width']  = [dim || 'See Description'];
        if (!aspects['Item Length']) aspects['Item Length'] = [dim || 'See Description'];
        if (!aspects['Item Height']) aspects['Item Height'] = ['See Description'];
      }
    }
    }
    if (!aspects['Country/Region of Manufacture']) aspects['Country/Region of Manufacture'] = ['China'];
    // Truncate all aspect values to eBay's 65-char limit
    for (const [k, v] of Object.entries(aspects)) {
      if (Array.isArray(v)) aspects[k] = v.map(s => String(s).slice(0, 65));
    }
    console.log('[push] aspects filled:', Object.keys(aspects).length, 'fields');

    // eBay hard limit: max 45 item specifics per listing
    // Priority: keep required/high-value fields, drop low-value extras
    if (Object.keys(aspects).length > 45) {
      const PRIORITY_KEEP = new Set(['Brand','Color','Size','Material','Style','Department','Type',
        'Pattern','Occasion','Size Type','Country/Region of Manufacture','MPN','UPC','EAN','ISBN',
        'Set Includes','Number of Items in Set','Room','Assembly Required','Theme','Age Group',
        'Gender','Fabric Type','Care Instructions','Fit','Neckline','Sleeve Length','Closure',
        'Features','Compatible Model','Compatible Brand','Connectivity','Wattage','Voltage',
        'Capacity','Shape','Seating Capacity','Number of Pieces','Bra Size',
        'Chest Size','Waist Size','Inseam','Leg Style']);
      const keys = Object.keys(aspects);
      const keep = keys.filter(k => PRIORITY_KEEP.has(k));
      const extra = keys.filter(k => !PRIORITY_KEEP.has(k));
      // Fill up to 45 with priority first, then extras
      const allowed = new Set([...keep, ...extra].slice(0, 45));
      for (const k of keys) { if (!allowed.has(k)) delete aspects[k]; }
      console.log(`[push] aspects capped from ${keys.length} to ${Object.keys(aspects).length} (eBay limit 45)`);
    }
  }
  // Truncate ALL aspect values to 65 chars max (eBay hard limit)
  for (const k of Object.keys(aspects)) {
    if (Array.isArray(aspects[k])) {
      aspects[k] = aspects[k].map(v => typeof v === 'string' && v.length > 65 ? v.slice(0, 62) + '...' : v);
    }
  }

  const _isAE = !!(body._source === 'aliexpress' || body.sourceUrl?.includes('aliexpress'));
  const locationKey = await ensureLocation(auth, _isAE);
  // When revising (existingEbaySku set), MUST reuse the same groupSku
  // eBay maps groupSku → listingId permanently — a new random groupSku = new listing
  const groupSku    = body._forceGroupSku
    || body.existingEbaySku   // revise: reuse existing listing's SKU
    || `DS-${Date.now()}-${Math.random().toString(36).slice(2,7).toUpperCase()}`; // fresh push: new SKU
  const basePrice   = parseFloat(product.price || product.cost || product.myPrice || 0);

  // ── SIMPLE ────────────────────────────────────────────────────────────────
  if (!product.hasVariations || !product.variations?.length) {
    const ebayPrice = applyMk(basePrice);
    const simpleQty = product.inStock !== false ? defaultQty : 0;
    // ── HARD PRICE FLOOR ─────────────────────────────────────────────────────
    // Refuse to publish a buyable listing below $1. Skip this check when
    // qty=0 — a $0 listing with qty=0 is unbuyable and is the intentional
    // OOS placeholder shape from the Amazon-$0 force-OOS block above.
    if (simpleQty > 0 && !(ebayPrice >= 1)) {
      console.warn(`[push/simple] BLOCKED: cost=$${basePrice} → ebayPrice=$${ebayPrice} below $1 floor with qty=${simpleQty} — refusing to publish a buyable $0 listing`);
      return res.status(400).json({
        error: `Computed eBay price ($${(ebayPrice||0).toFixed(2)}) is below the $1 minimum and qty would be ${simpleQty}. Refusing to publish a buyable listing at this price.`,
        code: 'PRICE_TOO_LOW'
      });
    }
    // For OOS (qty=0) listings with 0 ebayPrice, fall back to a $9.99 placeholder
    // so eBay accepts the offer ($0.99 minimum). Worker will revise on next pass.
    const finalPrice = (ebayPrice >= 1 ? ebayPrice : 9.99).toFixed(2);
    console.log(`[push/simple] cost=$${basePrice} → $${finalPrice} (qty=${simpleQty})`);

    console.log(`[push/simple] inStock=${product.inStock} qty=${simpleQty}`);
    const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(groupSku)}`, {
      method: 'PUT', headers: auth,
      body: JSON.stringify({
        availability: { shipToLocationAvailability: { quantity: simpleQty } },
        condition: 'NEW',
        product: { title: listingTitle, description: ebayDescription, imageUrls: product.images.slice(0, 12), aspects },
      }),
    });
    if (ir.status === 401) return res.status(401).json({ error: 'eBay token missing inventory permission. Go to Settings → Force Reconnect.', code: 'INVENTORY_401' });
    if (!ir.ok) return res.status(400).json({ error: 'Inventory PUT failed', details: await ir.text() });

    const or = await fetch(`${EBAY_API}/sell/inventory/v1/offer`, {
      method: 'POST', headers: auth,
      body: JSON.stringify(buildOffer(groupSku, finalPrice, categoryId, policies, locationKey)),
    });
    const od = await or.json();
    if (!or.ok) return res.status(400).json({ error: 'Offer creation failed', details: od });

    const pr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${od.offerId}/publish`, { method: 'POST', headers: auth });
    const pd = await pr.json();
    if (pd.listingId) return res.json({ success: true, sku: groupSku, offerId: od.offerId, listingId: pd.listingId });

    const errId = (pd.errors || [])[0]?.errorId;
    if (errId === 25002) {
      const existing = (pd.errors[0]?.parameters || []).find(p => p.name === 'listingId')?.value || null;
      const errMsg = pd.errors[0]?.message || '';
      if (existing) {
        // Confirmed real duplicate — already listed, return success so frontend marks it as listed
        // Note: createdSkus isn't declared in the simple path — simple listings have a single SKU,
        // so variantsCreated is always 1 when the listing exists.
        console.log(`[push] confirmed duplicate — already listed as ${existing}`);
        return res.json({ success: true, sku: groupSku, listingId: existing, variantsCreated: 1, duplicate: true });
      }
      // 25002 system error — retry
      console.warn('[push] 25002 system error on simple publish — retrying');
      for (let retry = 1; retry <= 2; retry++) {
        await sleep(retry * 1500);
        const pr2 = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${od.offerId}/publish`, { method: 'POST', headers: auth });
        const pd2 = await pr2.json();
        if (pd2.listingId) return res.json({ success: true, sku: groupSku, offerId: od.offerId, listingId: pd2.listingId });
        const e2 = (pd2.errors || [])[0];
        if (e2?.errorId === 25002 && (e2.parameters || []).find(p => p.name === 'listingId')?.value) {
          return res.json({ success: true, sku: groupSku, listingId: e2.parameters.find(p=>p.name==='listingId').value, duplicate: true });
        }
        console.warn(`[push] simple retry ${retry}: ${JSON.stringify(pd2).slice(0,150)}`);
      }
      return res.status(400).json({ error: 'Publish failed: eBay system error. Please try again.', errorId: 25002 });
    }
    return res.status(400).json({ error: 'Publish failed', details: pd });
  }

  // ── VARIATION ─────────────────────────────────────────────────────────────
  // Use stored dim names if available, else detect
  const primaryName   = product._primaryDimName   || null;
  const secondaryName = product._secondaryDimName || null;
  const colorGroup    = primaryName
    ? product.variations.find(v => v.name === primaryName)
    : (product.variations.find(v => /color|colour/i.test(v.name)) || product.variations[0]);
  const otherGroup    = secondaryName
    ? product.variations.find(v => v.name === secondaryName)
    : product.variations.find(v => v !== colorGroup);

  const variants = buildVariants({ product, groupSku, applyMk, defaultQty, body });
  if (!variants.length) return res.status(400).json({ error: 'No variants could be built — check that the product has valid variation values.' });

  // ── HARD PRICE FLOOR (variation path) ────────────────────────────────────
  // Refuse only if a BUYABLE variant (qty > 0) is below $1. Variants at qty=0
  // (out-of-stock placeholders from the Amazon-$0 force-OOS rule) are allowed
  // through with placeholder prices because no one can buy them.
  {
    const _badVariants = variants.filter(v => {
      const _qty = parseInt(v.qty) || 0;
      const _price = parseFloat(v.price) || 0;
      return _qty > 0 && _price < 1;
    });
    if (_badVariants.length) {
      const _sample = _badVariants.slice(0, 3).map(v => `${v.sku}=$${v.price}@qty${v.qty}`).join(', ');
      console.warn(`[push/var] BLOCKED: ${_badVariants.length}/${variants.length} BUYABLE variants below $1 floor. Sample: ${_sample}`);
      return res.status(400).json({
        error: `${_badVariants.length} of ${variants.length} variants would publish at qty>0 below the $1 minimum (e.g. ${_sample}). Refusing to publish buyable $0 variants.`,
        code: 'PRICE_TOO_LOW'
      });
    }
    // Bump any qty=0 variant whose computed price is below $1 to a $9.99
    // placeholder so eBay accepts the offer. They're unbuyable so price is moot.
    let _bumped = 0;
    for (const v of variants) {
      const _qty = parseInt(v.qty) || 0;
      const _price = parseFloat(v.price) || 0;
      if (_qty === 0 && _price < 1) {
        v.price = '9.99';
        _bumped++;
      }
    }
    if (_bumped > 0) console.log(`[push/var] bumped ${_bumped} OOS variant(s) to $9.99 placeholder for eBay min-price compliance`);
  }

  // Block push if per-variant prices couldn't be verified
  // (all prices identical = Amazon blocked per-ASIN fetches, can't price accurately)
  if (product._pricesFailed) {
    const allPrices = [...new Set(variants.map(v => v.price))];
    if (allPrices.length === 1) {
      return res.status(400).json({
        error: `Cannot push — Amazon blocked per-variant price lookups. All ${variants.length} variants would be priced identically at $${allPrices[0]}, which is incorrect. Re-import this product later or push manually with correct prices.`,
        code: 'PRICES_FAILED',
        suggestion: 'Try importing this product again in a few minutes. If the issue persists, this listing type requires manual pricing.',
      });
    }
  }

  console.log(`[push] ${variants.length} variants (${colorGroup ? colorGroup.values.length : 0} colors × ${otherGroup ? otherGroup.values.length : 1} sizes)`);

  // PUT inventory items — test first item for 401, then batch the rest
  const createdSkus = new Set();

  async function putInventoryItem(v) {
    const itemAspects = { ...aspects };
    for (const [k, val] of Object.entries(v.dims)) {
      // Normalize dim key to match eBay taxonomy
      const normKey = /color|colour/i.test(k) ? 'Color' : /^size$/i.test(k) ? 'Size' : k;
      itemAspects[normKey] = [val];
    }
    const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(v.sku)}`, {
      method: 'PUT', headers: auth,
      body: JSON.stringify({
        availability: { shipToLocationAvailability: { quantity: v.qty } },
        condition: 'NEW',
        product: {
          title:       listingTitle,
          description: ebayDescription,
          imageUrls:   (() => {
            // For 3-dim listings: each (Fit_type+Color) combo = unique ASIN = unique images
            // Use asinImages[asin] directly — most accurate, no cycling/guessing
            const skuAsin = v.asin; // set on variant during buildVariants for fully-multi-asin listings
            if (skuAsin && product.asinImages?.[skuAsin]?.length) {
              return product.asinImages[skuAsin].slice(0, 12);
            }
            // Fallback: per-primary-value image array from variationImages
            const primDimName = product._primaryDimName || 'Color';
            const pv = v.dims?.[primDimName] || v.dims?.['Color'] || v.dims?.['Style'] || Object.values(v.dims || {})[0];
            const raw = pv
              ? (product.variationImages?.[primDimName]?.[pv]
                || product.variationImages?.['Color']?.[pv]
                || product.variationImages?.['Style']?.[pv]
                || Object.values(product.variationImages||{}).reduce((imgs,m)=>imgs.length?imgs:(Array.isArray(m[pv])?m[pv]:(m[pv]?[m[pv]]:[])),[]))
              : [];
            const imgs = Array.isArray(raw) ? raw : (raw ? [raw] : []);
            if (!imgs.length && v.image) return [v.image];
            if (!imgs.length) return product.images?.slice(0, 1) || [];
            return imgs.slice(0, 12);
          })(),
          aspects:     itemAspects,
        },
      }),
    });
    return r;
  }

  // Test first
  const testR = await putInventoryItem(variants[0]);
  if (testR.status === 401) return res.status(401).json({ error: 'eBay token missing inventory permission. Go to Settings → Force Reconnect.', code: 'INVENTORY_401' });
  if (testR.ok || testR.status === 204) createdSkus.add(variants[0].sku);
  else console.warn(`[push] first item failed: ${testR.status} ${(await testR.text()).slice(0, 100)}`);

  // Batch the rest in groups of 15
  for (let i = 1; i < variants.length; i += 15) {
    await Promise.all(variants.slice(i, i + 15).map(async v => {
      const r = await putInventoryItem(v);
      if (r.ok || r.status === 204) createdSkus.add(v.sku);
      else console.warn(`[push] inv fail ${v.sku.slice(-20)}: ${r.status}`);
    }));
    if (i + 15 < variants.length) await sleep(100);
  }

  console.log(`[push] inventory items: ${createdSkus.size}/${variants.length}`);

  // Group PUT
  const groupAspects = { ...aspects };
  // Remove ALL variation dimensions from group aspects — eBay 25013 if same key in both
  // This includes Color, Size, and extra dims like Pattern, Style, Fit Type etc
  const _allVarDimNames = [
    ...(product.variations || []).map(vg => vg.name),
    ...(product._extraDimNames || []),
    ...(product._primaryDimName   ? [product._primaryDimName]   : []),
    ...(product._secondaryDimName ? [product._secondaryDimName] : []),
    'Color','color','colour','Colour','Size','size','Pattern','pattern','Style','style',
    'Fit Type','fit_type','Size Name','size_name','Color Name','color_name',
    'Pattern Name','pattern_name','Style Name','style_name',
  ];
  for (const _dvn of _allVarDimNames) {
    delete groupAspects[_dvn];
    delete groupAspects[_dvn.toLowerCase()];
  }
  // Pass capped variants so specs only include values that actually exist in kept variants
  const _cappedVars = variants.slice(0, 100);
  const _capVariants = _cappedVars.map(v => ({
    primaryVal:   colorGroup  ? v.dims?.[colorGroup.name]  : null,
    secondaryVal: otherGroup  ? v.dims?.[otherGroup.name]  : null,
    dims:         v.dims || {},  // pass full dims so buildVariesBy can read extraDimNames
  }));
  const variesBy = buildVariesBy(product, colorGroup, otherGroup, _capVariants);

  // Enforce strict spec/item consistency: only push variants whose color IS in the spec
  // When there are too many colors to fit in 65 chars joined, spec will have fewer colors
  // Any item with a color NOT in the spec causes 25013 — so remove those variants
  if (variesBy?.specifications && colorGroup) {
    const _colorSpecName = (n => !n?n:/color|colour/i.test(n)?'Color':/^size$/i.test(n)?'Size':n.replace('_name','').replace(/^\w/,c=>c.toUpperCase()))(colorGroup.name);
    const _specColors = new Set(
      variesBy.specifications.find(s => s.name === _colorSpecName)?.values || []
    );
    if (_specColors.size > 0) {
      const _beforeCount = _cappedVars.length;
      // Keep only variants whose color matches a spec value
      const _filtered = _cappedVars.filter(v => {
        const c = v.dims?.[colorGroup.name];
        return !c || _specColors.has(c); // keep if no color dim or color is in spec
      });
      if (_filtered.length < _beforeCount) {
        console.log(`[push] spec-filtered variants: ${_filtered.length}/${_beforeCount} kept (${_specColors.size} spec colors)`);
        // Replace variants array with filtered version
        variants.length = 0;
        for (const v of _filtered) variants.push(v);
      }
    }
  }
  // For FULLY-MULTI-ASIN: restrict variesBy specs to real combo values only
  // Prevents "9 colors × 17 sizes" when only 22 combos actually exist (rest would be phantom)
  if (product._isFullyMultiAsin && variants.length > 0) {
    const _realPrimVals = [...new Set(variants.map(v => v.primaryVal).filter(Boolean))];
    const _realSecVals  = [...new Set(variants.map(v => v.secondaryVal).filter(Boolean))];
    if (variesBy?.specifications) {
      for (let _si = 0; _si < variesBy.specifications.length; _si++) {
        const realVals = _si === 0 ? _realPrimVals : _realSecVals;
        if (realVals.length > 0 && realVals.length < variesBy.specifications[_si].values.length) {
          variesBy.specifications[_si].values = realVals.map(v => String(v).slice(0,65).trim()).filter(Boolean);
        }
      }
    }
  }

  // Validate: all specs must have at least 1 value
  const validSpecs = variesBy.specifications.filter(s => s.values?.length > 0);
  if (!validSpecs.length) return res.status(400).json({ error: 'No valid variation specifications — product needs at least one variation value.' });
  // Deduplicate specs by name (eBay error 25013 if same name appears twice)
  const seenSpecNames = new Set();
  variesBy.specifications = validSpecs.filter(s => {
    if (seenSpecNames.has(s.name)) { console.warn(`[push] duplicate spec "${s.name}" removed`); return false; }
    seenSpecNames.add(s.name); return true;
  });
  console.log(`[push] variesBy specs: ${JSON.stringify(validSpecs.map(s=>({name:s.name,count:s.values.length})))}`);

  let groupOk = false;
  // Group gallery = product.images (12 photos of default/main color)
  // eBay shows these in the main listing; color-switching images come from per-variant imageUrls
  const groupImageUrls = [...new Set((product.images || []).filter(u => u && typeof u === 'string' && u.startsWith('http')))].slice(0, 12);
  if (!groupImageUrls.length) {
    // fallback: first available color image
    const anyImg = Object.values(product.variationImages || {}).flatMap(m => Object.values(m)).find(Boolean) || '';
    if (anyImg) groupImageUrls.push(anyImg);
  }
  console.log(`[push] group images: ${groupImageUrls.length}`);

  // Fit variesBy spec values so joined string ≤65 chars (eBay error 25718)
  // Cap each spec value at 65 chars individually — include ALL values (no total-length dropping)
  // The 65-char TOTAL joined limit was wrong: caused 25013 by dropping colors from spec
  const _fitSpec25718 = (specs) => {
    if (!specs) return specs;
    return specs.map(spec => {
      if (!spec.values?.length) return spec;
      const unique = [...new Set(spec.values.map(v => String(v||'').slice(0,65).trim()).filter(Boolean))];
      return { ...spec, values: unique };
    });
  };
  if (variesBy?.specifications) variesBy.specifications = _fitSpec25718(variesBy.specifications);

  // Pre-flight: clamp variant prices to within 4X of cheapest (eBay 25019 prevention)
  {
    const prices = variants.map(v => parseFloat(v.price)).filter(p => p > 0);
    if (prices.length > 1) {
      const minP = Math.min(...prices);
      const maxAllowed = +(minP * 4).toFixed(2);
      let clamped = 0;
      for (const v of variants) {
        if (parseFloat(v.price) > maxAllowed) { v.price = String(maxAllowed); clamped++; }
      }
      if (clamped > 0) console.log(`[push] pre-flight price clamp: min=$${minP} maxAllowed=$${maxAllowed} clamped=${clamped} variants`);
    }
  }

  const _buildGroupBody = () => {
    // Always strip current variesBy spec names from aspects before PUT
    // Spec names can change during 25013 reconcile — must stay in sync
    const _cleanAspects = { ...groupAspects };
    for (const _spec of (variesBy?.specifications || [])) {
      delete _cleanAspects[_spec.name];
      delete _cleanAspects[_spec.name?.toLowerCase?.()];
    }
    return JSON.stringify({
      inventoryItemGroupKey: groupSku,
      title:       listingTitle,
      description: ebayDescription,
      imageUrls:   groupImageUrls,
      variantSKUs: variants.map(v => v.sku).filter(s => createdSkus.has(s)),
      aspects:     _cleanAspects,
      variesBy,
    });
  };
  for (let attempt = 1; attempt <= 5 && !groupOk; attempt++) {
    const gr = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`, {
      method: 'PUT', headers: auth, body: _buildGroupBody(),
    });
    if (gr.ok || gr.status === 204) { groupOk = true; console.log('[push] group ok'); }
    else {
      const gt = await gr.text();
      console.warn(`[push] group attempt ${attempt}: ${gr.status} ${gt.slice(0, 300)}`);
      // 25718: spec values too long — already trimmed above, shouldn't repeat, but just in case
      const gErr = JSON.parse(gt || '{}');
      if ((gErr.errors||[]).some(e => e.errorId === 25718)) {
        // Already applied _fitSpec25718 — nothing more to do, will fail
        return res.status(400).json({ error: 'Group PUT failed: spec values too long (eBay 25718)', details: gt.slice(0,300) });
      }
      if (attempt < 5) await sleep([0, 1000, 3000, 5000, 8000][attempt]);
      else return res.status(400).json({ error: 'Group PUT failed', details: gt.slice(0, 400) });
    }
  }

  // Bulk create offers
  const allOfferIds = [];
  const offerIdsBySku = {}; // sku → offerId (for 25019 repricing)
  const failedOfferVariants = [];
  for (let i = 0; i < variants.length; i += 25) {
    const batch = variants.slice(i, i + 25).map(v => buildOffer(v.sku, v.price, categoryId, policies, locationKey));
    const or = await fetch(`${EBAY_API}/sell/inventory/v1/bulk_create_offer`, {
      method: 'POST', headers: auth, body: JSON.stringify({ requests: batch }),
    });
    const od = await or.json();
    for (const resp of (od.responses || [])) {
      if (resp.offerId) { allOfferIds.push(resp.offerId); if (resp.sku) offerIdsBySku[resp.sku] = resp.offerId; }
      else {
        console.warn(`[push] offer fail ${resp.sku?.slice(-20)}: ${JSON.stringify(resp.errors?.[0]).slice(0, 150)}`);
        failedOfferVariants.push(variants.find(v => v.sku === resp.sku));
      }
    }
  }

  // Retry failed offers after 4s (location key propagation delay)
  if (failedOfferVariants.length) {
    await sleep(4000);
    for (let i = 0; i < failedOfferVariants.length; i += 25) {
      const batch = failedOfferVariants.filter(Boolean).slice(i, i + 25).map(v => buildOffer(v.sku, v.price, categoryId, policies, locationKey));
      const or = await fetch(`${EBAY_API}/sell/inventory/v1/bulk_create_offer`, {
        method: 'POST', headers: auth, body: JSON.stringify({ requests: batch }),
      });
      const od = await or.json();
      for (const resp of (od.responses || [])) {
        if (resp.offerId) { allOfferIds.push(resp.offerId); if (resp.sku) offerIdsBySku[resp.sku] = resp.offerId; }
      }
    }
  }

  console.log(`[push] offers created: ${allOfferIds.length}/${variants.length}`);

  // ── Pre-fill required aspects from eBay category API before publish ──────────
  // Fetches all REQUIRED aspects for this category and fills any missing ones.
  // This prevents 25002 "X is missing" from firing on each publish attempt separately.
  try {
    const _catAspR = await fetch(
      `${EBAY_API}/commerce/taxonomy/v1/category_tree/0/get_item_aspects_for_category?category_id=${categoryId}`,
      { headers: { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json' } }
    );
    if (_catAspR.ok) {
      const _catAspD = await _catAspR.json();
      const _reqAspects = (_catAspD.aspects || []).filter(a =>
        a.aspectConstraint?.aspectRequired === true && !a.aspectConstraint?.itemToAspectCardinality?.includes('MULTI')
      );
      const _title = (product.title || '').toLowerCase();
      let _preFilled = 0;
      for (const _asp of _reqAspects) {
        const _name = _asp.aspectName;
        if (aspects[_name]) continue; // already filled
        // Try to match from product specs first
        const _specVal = (product.aspects || {})[_name] || (product.itemSpecifics || {})[_name];
        if (_specVal) { aspects[_name] = Array.isArray(_specVal) ? _specVal : [String(_specVal)]; _preFilled++; continue; }
        // Use allowed values if available
        const _allowed = _asp.aspectValues?.map(v => v.localizedValue).filter(Boolean) || [];
        if (_allowed.length === 1) { aspects[_name] = [_allowed[0]]; _preFilled++; continue; }
        // Smart defaults by aspect name
        if (/^Model$/i.test(_name)) {
          const _w = (product.title||'').replace(/[,|]/g,' ').split(/\s+/).filter(w=>w.length>1);
          aspects[_name] = [_w.slice(0,5).join(' ').slice(0,65) || 'See Description']; _preFilled++;
        } else if (/^(Band|Strap) Material$/i.test(_name)) {
          aspects[_name] = [/silicone/i.test(_title)?'Silicone':/leather/i.test(_title)?'Leather':/nylon/i.test(_title)?'Nylon':'Silicone']; _preFilled++;
        } else if (/^Compatible Operating System$/i.test(_name)) {
          aspects[_name] = [/android/i.test(_title)?'Android':(/apple|ios|iphone|watch se|watch ultra/i.test(_title)?'iOS':'Android, iOS')]; _preFilled++;
        } else if (/^Case Size$/i.test(_name)) {
          const _mm = (product.title||'').match(/(\d{2})\s*mm/i);
          aspects[_name] = [_mm ? _mm[1]+' mm' : '40 mm']; _preFilled++;
        } else if (/^MPN$/i.test(_name)) {
          aspects[_name] = ['Does Not Apply']; _preFilled++;
        } else if (/^(Type|Watch Style)$/i.test(_name)) {
          aspects[_name] = ['Smartwatch']; _preFilled++;
        } else if (/^Display Technology$/i.test(_name)) {
          aspects[_name] = ['OLED']; _preFilled++;
        } else if (/^Connectivity$/i.test(_name)) {
          aspects[_name] = ['Bluetooth']; _preFilled++;
        } else if (/^Features$/i.test(_name)) {
          aspects[_name] = ['Heart Rate Monitor']; _preFilled++;
        } else if (_allowed.length > 0) {
          aspects[_name] = [_allowed[0]]; _preFilled++;
        } else {
          aspects[_name] = ['See Description']; _preFilled++;
        }
      }
      if (_preFilled > 0) {
        console.log(`[push] pre-filled ${_preFilled} required aspects from category API`);
        // Sync to groupAspects
        for (const k of Object.keys(aspects)) { if (!groupAspects[k]) groupAspects[k] = aspects[k]; }
        // Re-PUT group with pre-filled aspects
        await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`,
          { method: 'PUT', headers: auth, body: _buildGroupBody() }
        ).catch(() => null);
      }
    }
  } catch(e) { console.warn('[push] pre-fill aspects error:', e.message); }

  // Publish group
  for (let attempt = 1; attempt <= 5; attempt++) {
    const pr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/publish_by_inventory_item_group`, {
      method: 'POST', headers: auth,
      body: JSON.stringify({ inventoryItemGroupKey: groupSku, marketplaceId: 'EBAY_US' }),
    });
    const pd = await pr.json();
    if (pd.listingId) {
      console.log(`[push] published! listingId=${pd.listingId} variants=${variants.length}`);

      // ── Post-publish revise: set correct per-variant prices + quantities ─────
      // Uses comboPrices from fresh scrape — every variant gets its exact price.
      // bulk_update_price_quantity is the lightest API call: no inventory item PUT needed.
      // CRITICAL: declared OUTSIDE the try block so the offerIdCache builder
      // below at line ~4436 can read it. Previous version had it inside the try
      // and crashed the entire Node process with `ReferenceError: _ppOfferMap is
      // not defined` because const has block scope. Two Railway crashes during
      // bulk push were both this exact line. Initialize empty so the ref is safe
      // even if the post-publish revise block throws.
      const _ppOfferMap = {}; // sku → { offerId, price, qty }
      try {
        // Get all offer IDs for published SKUs
        for (let _ppi = 0; _ppi < variants.length; _ppi += 20) {
          await Promise.all(variants.slice(_ppi, _ppi + 20).map(async v => {
            try {
              const _or = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(v.sku)}`, { headers: auth });
              if (!_or.ok) return;
              const _od = await _or.json().catch(() => ({}));
              const _oid = (_od.offers || [])[0]?.offerId;
              if (_oid) _ppOfferMap[v.sku] = { offerId: _oid, price: v.price, qty: parseInt(v.qty) || 0 };
            } catch(e) {}
          }));
        }
        const _ppEntries = Object.values(_ppOfferMap);
        if (_ppEntries.length) {
          // bulk_update_price_quantity: set correct price + qty for all variants at once
          for (let _bi = 0; _bi < _ppEntries.length; _bi += 25) {
            const batch = _ppEntries.slice(_bi, _bi + 25).map(e => ({
              offerId: e.offerId,
              price: { value: e.price, currency: 'USD' },
              availableQuantity: e.qty,
            }));
            await fetch(`${EBAY_API}/sell/inventory/v1/bulk_update_price_quantity`, {
              method: 'POST', headers: auth, body: JSON.stringify({ requests: batch }),
            }).catch(() => {});
          }
          console.log(`[push] post-publish revise: updated ${_ppEntries.length} offer prices+quantities`);
        }
      } catch(e) { console.warn('[push] post-publish revise error:', e.message); }

      // Build skuToAsin — maps each pushed eBay SKU directly to its Amazon ASIN
      // Revise uses this to fetch the exact same ASINs without any comboKey decoding
      const _skuToAsin = {};
      for (const v of variants) {
        const _asin = product.comboAsin?.[v.dimKey];
        if (_asin) _skuToAsin[v.sku] = _asin;
      }
      console.log(`[push] built skuToAsin: ${Object.keys(_skuToAsin).length} entries`);

      // Build flat offerIdCache: { sku: offerId } — saved to product so smartSync uses it immediately
      const _offerIdCache = {};
      for (const [sku, entry] of Object.entries(_ppOfferMap)) {
        if (entry.offerId) _offerIdCache[sku] = entry.offerId;
      }

      return res.json({ success: true, sku: groupSku, listingId: pd.listingId, variantsCreated: variants.length,
        skuToAsin: _skuToAsin,
        offerIdCache: _offerIdCache,
        newProduct: { _primaryDimName: product._primaryDimName, _secondaryDimName: product._secondaryDimName, _extraDimNames: product._extraDimNames || [], _isFullyMultiAsin: product._isFullyMultiAsin || false, variations: product.variations, price: product.price, images: product.images, comboPrices: product.comboPrices, comboAsin: product.comboAsin, comboInStock: product.comboInStock, comboShipping: product.comboShipping } });
    }
    const errId = (pd.errors || [])[0]?.errorId;
    const errMsg0 = (pd.errors || [])[0]?.message || '';
    console.warn(`[push] publish attempt ${attempt}: ${JSON.stringify(pd).slice(0, 300)}`);
    // 25604/25705: Group not found — previous cleanup deleted the group → broken mapping
    // Delete offers + group completely, re-create fresh, publish as new (gets new listingId)
    if (errId === 25604 || errId === 25705 || /product not found|group.*not found/i.test(errMsg0)) {
      console.warn(`[push] ${errId} group/product not found — cleaning up orphaned offers and re-creating`);
      // Delete any orphaned offers for these variant SKUs
      for (const v of variants) {
        const _oR = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(v.sku)}`, { headers: auth })
          .then(r => r.json()).catch(() => ({}));
        for (const _o of (_oR.offers || [])) {
          await fetch(`${EBAY_API}/sell/inventory/v1/offer/${_o.offerId}`, { method: 'DELETE', headers: auth }).catch(() => {});
        }
      }
      // Delete and re-create the group from scratch
      await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`,
        { method: 'DELETE', headers: auth }).catch(() => null);
      await sleep(1000);
      await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`,
        { method: 'PUT', headers: auth, body: _buildGroupBody() }).catch(() => null);
      // Re-create offers
      const _reOfferReqs = variants.filter(v => createdSkus.has(v.sku)).slice(0, 100).map(v =>
        buildOffer(v.sku, parseFloat(v.price) > 0 ? parseFloat(v.price).toFixed(2) : '0',
          categoryId, policies, locationKey)
      );
      for (let _roi = 0; _roi < _reOfferReqs.length; _roi += 25) {
        await fetch(`${EBAY_API}/sell/inventory/v1/bulk_create_offer`,
          { method: 'POST', headers: auth, body: JSON.stringify({ requests: _reOfferReqs.slice(_roi, _roi+25) }) }
        ).catch(() => null);
      }
      // Clear cached offer IDs in place (const, can't reassign)
      for (const _ok of Object.keys(offerIdsBySku)) delete offerIdsBySku[_ok];
      console.log(`[push] ${errId} recovery: offers re-created, retrying publish`);
      if (attempt < 5) { await sleep(3000); continue; }
      return res.status(400).json({ error: 'eBay group not found (25705) — try re-pushing', errorId: errId });
    }

    // 25013: variation specs mismatch — group specs don't match inventory items
    // Fix: re-PUT the group with exact values from the actual inventory items
    if (errId === 25013) {
      console.warn(`[push] 25013 specs mismatch — reconciling group specs with inventory items`);
      // Collect exact dim values from all inventory items we created
      const exactPrimVals = [...new Set(variants.map(v => v.dims?.[colorGroup?.name]).filter(Boolean))];
      const exactSecVals  = otherGroup ? [...new Set(variants.map(v => v.dims?.[otherGroup?.name]).filter(Boolean))] : [];
      if (exactPrimVals.length && variesBy?.specifications) {
        // Rebuild specs with exact values (no 65-char truncation for the list)
        const newSpecs = [];
        // Inline normSpecName since it's out of scope here
        const _nsn = n => !n ? n : /color|colour/i.test(n) ? 'Color' : /^size$/i.test(n) ? 'Size' : n.replace('_name','').replace(/^\w/, c=>c.toUpperCase());
        if (colorGroup && exactPrimVals.length) {
          const trimmed = exactPrimVals.map(v => String(v).slice(0,30).trim());
          const fitted = []; let len = 0;
          for (const v of trimmed) {
            const add = (fitted.length ? 3 : 0) + v.length;
            if (len + add > 65) break;
            fitted.push(v); len += add;
          }
          if (fitted.length) newSpecs.push({ name: _nsn(colorGroup.name), values: fitted });
        }
        if (otherGroup && exactSecVals.length) {
          const trimmed2 = exactSecVals.map(v => String(v).slice(0,30).trim());
          const fitted2 = []; let len2 = 0;
          for (const v of trimmed2) {
            const add = (fitted2.length ? 3 : 0) + v.length;
            if (len2 + add > 65) break;
            fitted2.push(v); len2 += add;
          }
          if (fitted2.length) newSpecs.push({ name: _nsn(otherGroup.name), values: fitted2 });
        }
        // Add extra dims (3rd, 4th, ...) from _extraDimNames
        const _extraDimNames25013 = product._extraDimNames || [];
        for (const _edn of _extraDimNames25013) {
          const _eVals = [...new Set(variants.map(v => v.dims?.[_edn]).filter(Boolean))];
          const _eTrimmed = _eVals.map(v => String(v).slice(0,30).trim());
          const _eFitted = []; let _eLen = 0;
          for (const v of _eTrimmed) {
            const add = (_eFitted.length ? 3 : 0) + v.length;
            if (_eLen + add > 65) break;
            _eFitted.push(v); _eLen += add;
          }
          if (_eFitted.length) newSpecs.push({ name: _nsn(_edn), values: _eFitted });
        }

        if (newSpecs.length) {
          variesBy.specifications = newSpecs;
          console.log(`[push] 25013 reconciled specs: ${JSON.stringify(newSpecs.map(s=>({name:s.name,count:s.values.length})))}`);
          // Build allowed color set from fitted spec
          const _allowedColors = new Set(newSpecs.find(s=>/color/i.test(s.name))?.values || []);
          // Filter variants to only those with colors in spec
          const _matchingVariants = _allowedColors.size > 0
            ? variants.filter(v => {
                const vc = v.dims?.[colorGroup?.name];
                return !vc || _allowedColors.has(vc);
              })
            : variants;
          // Trim variants array AND delete non-matching inventory items
          // _buildGroupBody() uses variants[] for variantSKUs — must match spec exactly
          if (_matchingVariants.length < variants.length) {
            const _droppedSkus = variants.filter(v => !_matchingVariants.includes(v)).map(v => v.sku);
            console.log(`[push] 25013 filtering items: ${_matchingVariants.length}/${variants.length} variants match spec — dropping ${_droppedSkus.length} non-matching SKUs`);
            // Delete non-matching inventory items so eBay group is consistent
            for (let _di = 0; _di < _droppedSkus.length; _di += 25) {
              const _delBatch = _droppedSkus.slice(_di, _di+25);
              await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item?sku=${_delBatch.map(encodeURIComponent).join(',')}`,
                { method: 'DELETE', headers: auth }).catch(()=>{});
            }
            // Trim variants so _buildGroupBody() only includes matching SKUs
            variants.splice(0, variants.length, ..._matchingVariants);
          }
          // Re-PUT the group with reconciled specs
          const regrR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`,
            { method: 'PUT', headers: auth, body: _buildGroupBody() }
          ).catch(() => null);
          if (regrR?.ok || regrR?.status === 204) console.log(`[push] 25013 group re-PUT ok`);
          else console.warn(`[push] 25013 group re-PUT failed: ${regrR?.status}`);
        }
      }
      if (attempt < 5) { await sleep([0, 2000, 4000, 6000, 8000][attempt] || 3000); continue; }
      return res.status(400).json({ error: 'Publish failed: variation specs mismatch (eBay 25013). Try re-pushing.', errorId: 25013, unrecoverable: true });
    }
    // 25019: price variation > 4X — clamp all variant prices to within 4X of the min price
    if (errId === 25019 || /price difference.*variation|maximum price difference|4X|4x/i.test(errMsg0)) {
      const prices = variants.map(v => parseFloat(v.price)).filter(p => p > 0);
      const minP = Math.min(...prices);
      const maxAllowed = +(minP * 4).toFixed(2);
      let clamped = 0;
      for (const v of variants) {
        if (parseFloat(v.price) > maxAllowed) { v.price = String(maxAllowed); clamped++; }
      }
      console.log(`[push] 25019 price clamp: min=$${minP} maxAllowed=$${maxAllowed} clamped=${clamped}`);
      if (clamped > 0 && attempt < 5) {
        // Bulk reprice clamped offers
        const repriceReqs = variants
          .filter(v => offerIdsBySku?.[v.sku])
          .map(v => ({ offerId: offerIdsBySku[v.sku], availableQuantity: parseInt(v.qty)||1, pricingSummary: { price: { value: v.price, currency: 'USD' } } }));
        for (let pi = 0; pi < repriceReqs.length; pi += 25) {
          await fetch(`${EBAY_API}/sell/inventory/v1/bulk_update_price_quantity`,
            { method: 'POST', headers: auth, body: JSON.stringify({ requests: repriceReqs.slice(pi, pi+25) }) }).catch(()=>{});
          if (pi + 25 < repriceReqs.length) await sleep(300);
        }
        await sleep(2000); continue;
      }
      return res.status(400).json({ error: 'Variant prices differ by more than 4X — eBay requires all variants within same price range.', errorId: 25019, unrecoverable: true });
    }
    // 25005: category doesn't support multi-variation — retry with fallback categories
    if (errId === 25005 || /does not support multi-variation|invalid category/i.test(errMsg0)) {
      // Known variation-compatible fallback categories by product type
      const _title = (product?.title || listingTitle || '').toLowerCase();
      const _catFallbacks = [
        // Automotive → generic auto accessories
        _title.match(/wiper|windshield|rain-x|blade/i) ? '33559' : null,           // Windshield Wipers & Washers
        _title.match(/seat cover|car seat/i) ? '33553' : null,                     // Seat Covers
        _title.match(/car|auto|truck|vehicle|motor/i) ? '6030' : null,             // Other Auto Parts
        // Home & tools
        _title.match(/switch|light switch|dimmer|smart switch/i) ? '20685' : null, // Wall Light Switches
        _title.match(/rug|carpet|mat/i) ? '20580' : null,                          // Rugs & Carpets
        // Generic multi-variation safe categories
        '57974', // Sporting Goods > Other
        '9355',  // Clothing, Shoes > Men > Other
        '88433', // Home & Garden > Other
        '43551', // Everything Else > Other
      ].filter(Boolean);

      const _nextCat = _catFallbacks[attempt - 1] || _catFallbacks[_catFallbacks.length - 1];
      if (_nextCat && _nextCat !== categoryId) {
        categoryId = _nextCat;
        console.log(`[push] 25005 category fallback → ${categoryId}`);
        // Update all offers with the new category
        const _offerSkus = Object.keys(offerIdsBySku || {});
        for (let _oi = 0; _oi < _offerSkus.length; _oi += 10) {
          await Promise.all(_offerSkus.slice(_oi, _oi+10).map(async sku => {
            const oid = offerIdsBySku[sku];
            if (!oid) return;
            try {
              const or = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${oid}`, { headers: auth });
              if (!or.ok) return;
              const od = await or.json().catch(()=>({}));
              const upd = { ...od, categoryId: categoryId };
              delete upd.offerId; delete upd.listing; delete upd.status; delete upd.marketplaceId;
              await fetch(`${EBAY_API}/sell/inventory/v1/offer/${oid}`,
                { method: 'PUT', headers: auth, body: JSON.stringify(upd) }).catch(()=>{});
            } catch(e) {}
          }));
        }
        if (attempt < 5) { await sleep(2000); continue; }
      }
      return res.status(400).json({ error: `eBay category does not support variations (25005). Try a different category or push as single items.`, errorId: 25005, unrecoverable: true });
    }

    // 25016: price invalid — some offers have price=0 (blocked ASINs).
    // Fix: find and delete the zero-price offers, then retry with only priced variants.
    if (errId === 25016 || /price value is invalid|below the minimum price/i.test(errMsg0)) {
      const _zeroOffers = variants.filter(v => parseFloat(v.price) <= 0 && offerIdsBySku?.[v.sku]);
      const _pricedOffers = variants.filter(v => parseFloat(v.price) > 0);
      console.log(`[push] 25016 price=0: ${_zeroOffers.length} zero-price offers, ${_pricedOffers.length} priced — deleting zeros`);
      if (_zeroOffers.length === 0) {
        // All prices are supposedly valid — wait and retry (transient eBay lag)
        if (attempt < 5) { await sleep([0,2000,4000,6000,8000][attempt]||3000); continue; }
        return res.status(400).json({ error: 'eBay rejected prices (25016) — all prices appear valid, may be transient. Try re-pushing.', errorId: 25016 });
      }
      if (_pricedOffers.length === 0) {
        // All variants are OOS — can't publish at all
        return res.status(400).json({ error: `All ${variants.length} variants have price=0 — Amazon blocked all ASIN lookups. Try re-scraping later when Amazon unblocks.`, errorId: 25016, allBlocked: true });
      }
      // Delete zero-price offers so eBay doesn't see them
      for (const v of _zeroOffers) {
        const oid = offerIdsBySku[v.sku];
        await fetch(`${EBAY_API}/sell/inventory/v1/offer/${oid}`, { method: 'DELETE', headers: auth }).catch(()=>{});
        delete offerIdsBySku[v.sku];
      }
      // Remove zero-price variants from the group SKU list
      const _keptSkus = _pricedOffers.map(v => v.sku);
      const _updGroup = { ..._buildGroupBody() };
      try {
        const _grpCurr = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
        if (_grpCurr.variantSKUs) {
          _grpCurr.variantSKUs = _keptSkus;
          await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`,
            { method: 'PUT', headers: auth, body: JSON.stringify(_grpCurr) }).catch(()=>{});
        }
      } catch(e) {}
      console.log(`[push] 25016 removed zeros — retrying publish with ${_pricedOffers.length} priced variants`);
      if (attempt < 5) { await sleep(2000); continue; }
      return res.status(400).json({ error: `25016: ${_zeroOffers.length} zero-price offers blocked publish. Removed them — re-push to list ${_pricedOffers.length} priced variants.`, errorId: 25016 });
    }

    // 25009: return policy issue — re-fetch and update offers
    if (errId === 25009 || /return.*policy|return.*option.*missing/i.test(errMsg0)) {
      console.warn(`[push] 25009 return policy issue`);
      if (attempt < 5) { await sleep([0,2000,4000,6000,8000][attempt]||2000); continue; }
      return res.status(400).json({ error: 'Return policy issue — check your eBay return policy settings.', errorId: 25009, unrecoverable: true });
    }
    if (errId === 25002) {
      const existing = (pd.errors[0]?.parameters || []).find(p => p.name === 'listingId')?.value || null;
      const errMsg = pd.errors[0]?.message || '';
      if (existing) {
        console.log(`[push] confirmed duplicate — already listed as ${existing}`);
        return res.json({ success: true, sku: groupSku, listingId: existing, variantsCreated: createdSkus.size, duplicate: true });
      }
      // Check for "too many item specifics" — trim aspects and retry immediately
      if (/too many item specific|reduce the number/i.test(errMsg)) {
        const limitM = errMsg.match(/(\d+)\s+or\s+less/);
        const maxAspects = limitM ? parseInt(limitM[1]) : 45;
        const curKeys = Object.keys(aspects);
        if (curKeys.length > maxAspects) {
          const KEEP = new Set(['Brand','Color','Size','Material','Style','Department','Type','Pattern',
            'Set Includes','Number of Items in Set','Room','MPN','UPC','EAN','Assembly Required',
            'Occasion','Size Type','Country/Region of Manufacture','Features','Age Group','Gender',
            'Connectivity','Wattage','Voltage','Capacity','Shape','Seating Capacity','Bra Size']);
          const keep = curKeys.filter(k => KEEP.has(k));
          const extra = curKeys.filter(k => !KEEP.has(k));
          const allowed = new Set([...keep, ...extra].slice(0, maxAspects));
          for (const k of curKeys) { if (!allowed.has(k)) delete aspects[k]; }
          console.log(`[push] trimmed aspects to ${Object.keys(aspects).length} for retry`);
          continue; // retry with trimmed aspects
        }
      }
      // Check for "X is not allowed as variation specific" — rename the spec and retry
      if (/is not allowed as a variation specific/i.test(errMsg)) {
        const badSpecM = errMsg.match(/^([A-Za-z_][A-Za-z0-9_ ]*?) is not allowed/i);
        const badSpec = badSpecM?.[1]?.trim();
        if (badSpec && variesBy?.specifications) {
          console.log(`[push] renaming invalid variation spec "${badSpec}" to "Style"`);
          for (const spec of variesBy.specifications) {
            if (spec.name === badSpec) spec.name = 'Style';
          }
          // Also rename in each inventory item's aspects
          for (const v of variants) {
            if (v.dims?.[badSpec] !== undefined) {
              v.dims['Style'] = v.dims[badSpec];
              delete v.dims[badSpec];
            }
          }
          // Recreate inventory items with renamed spec
          const renamed = variants.slice(0, 100);
          for (let bi = 0; bi < renamed.length; bi += 25) {
            const batch = renamed.slice(bi, bi + 25);
            await fetch(`${EBAY_API}/sell/inventory/v1/bulk_create_or_replace_inventory_item`, {
              method: 'POST', headers: auth,
              body: JSON.stringify({ requests: batch.map(v => ({
                sku: v.sku,
                inventoryItem: { condition: 'NEW', product: { title: listingTitle, imageUrls: [v.image].filter(Boolean), aspects: { ...groupAspects, ...v.dims } }, availability: { shipToLocationAvailability: { quantity: parseInt(v.qty)||0 } }, pricingSummary: { price: { value: v.price, currency: 'USD' } } }
              })) })
            }).catch(() => {});
            if (bi + 25 < renamed.length) await sleep(200);
          }
          if (attempt < 5) { await sleep(2000); continue; }
        }
      }
      // Fall through to auto-fill block below — don't skip it with continue
      // auto-fill runs, then we sleep and retry
    }
    // Auto-fill any missing required aspects and retry (only for error 25002)
    const missingAspects = [];
    for (const err of (pd.errors || [])) {
      // Only auto-fill for 25002 missing aspect errors — skip 25009, 25019, 25005, etc.
      if (err.errorId !== 25002) continue;
      // Extract aspect name from the error message: "The item specific {AspectName} is missing..."
      const errMsgAspect = err.message || '';
      const aspectFromMsg = errMsgAspect.match(/item specific (.+?) is missing/i)?.[1]?.trim();
      // Also check parameters for direct aspect name
      for (const param of (err.parameters || [])) {
        const rawVal = param.value || '';
        // param.value can be full sentence "The item specific X is missing..." — extract X
        const aspectName = aspectFromMsg
          || rawVal.match(/item specific (.+?) is missing/i)?.[1]?.trim()
          || (rawVal.length < 80 && /^[A-Za-z][A-Za-z0-9 &().,-]{0,59}$/.test(rawVal.trim()) ? rawVal.trim() : null);
        if (!aspectName) continue;
        if (/^<|ReturnsAccepted|policy|PolicyId|listingId|sku/i.test(aspectName)) continue;
        // Skip catalog/system aspects that can't be filled with text values
        if (/^Product$|^GTIN$|^EAN$|^UPC$|^ISBN$|^MPN$|^Catalog/i.test(aspectName)) continue;
        if (!aspects[aspectName]) {
          missingAspects.push(aspectName);
          // Smart defaults for common required aspects
          const _tl = (product.title||'').toLowerCase();
          if (aspectName === 'Style') {
            if (/bra\b|bralette|wireless/i.test(_tl)) aspects[aspectName] = ['Wireless'];
            else if (/brief|boyshort|thong|panty/i.test(_tl)) aspects[aspectName] = ['Brief'];
            else if (/bikini|swimsuit/i.test(_tl)) aspects[aspectName] = ['Bikini'];
            else aspects[aspectName] = ['Other'];
          } else if (aspectName === 'Color') {
            aspects[aspectName] = ['Multicolor'];
          } else if (aspectName === 'Size') {
            aspects[aspectName] = ['One Size'];
          } else if (aspectName === 'Material') {
            const _mt = (product.title||'').toLowerCase();
            if (/stainless.steel|steel/i.test(_mt)) aspects[aspectName] = ['Stainless Steel'];
            else if (/aluminum|aluminium/i.test(_mt)) aspects[aspectName] = ['Aluminum'];
            else if (/plastic|acrylic/i.test(_mt)) aspects[aspectName] = ['Plastic'];
            else if (/cotton/i.test(_mt)) aspects[aspectName] = ['Cotton'];
            else if (/polyester/i.test(_mt)) aspects[aspectName] = ['Polyester'];
            else if (/nylon/i.test(_mt)) aspects[aspectName] = ['Nylon'];
            else if (/silicone/i.test(_mt)) aspects[aspectName] = ['Silicone'];
            else if (/rubber/i.test(_mt)) aspects[aspectName] = ['Rubber'];
            else if (/wood|bamboo/i.test(_mt)) aspects[aspectName] = ['Wood'];
            else if (/glass/i.test(_mt)) aspects[aspectName] = ['Glass'];
            else if (/foam/i.test(_mt)) aspects[aspectName] = ['Foam'];
            else if (/mesh/i.test(_mt)) aspects[aspectName] = ['Mesh'];
            else aspects[aspectName] = ['Synthetic'];
          } else if (aspectName === 'Model') {
            // Extract model from title — strip brand name and take the first meaningful phrase
            const titleWords = (product.title||'').replace(/[,|]/g,' ').split(/\s+/).filter(w => w.length > 1);
            aspects[aspectName] = [titleWords.slice(0, 5).join(' ').slice(0, 65) || 'See Description'];
          } else if (aspectName === 'MPN') {
            aspects[aspectName] = ['Does Not Apply'];
          } else if (aspectName === 'Color') {
            aspects[aspectName] = ['Multicolor'];
          } else if (aspectName === 'Capacity') {
            const capM = (product.title||'').match(/(\d+[\s-]*(?:cup|slice|qt|quart|oz|liter|l|gallon|gal))/i);
            aspects[aspectName] = [capM ? capM[1] : 'See Description'];
          } else if (aspectName === 'Wattage') {
            const wM = (product.title||'').match(/(\d{3,4})\s*[Ww](?:att)?/);
            aspects[aspectName] = [wM ? wM[1] + 'W' : 'See Description'];
          } else if (aspectName === 'Voltage') {
            aspects[aspectName] = ['120V'];
          } else if (aspectName === 'Set Includes') {
            const _ti = (product.title||'').toLowerCase();
            const pm = _ti.match(/(\d+)\s*(?:piece|pc)/);
            const n = pm ? parseInt(pm[1]) : 1;
            if (/dining/i.test(_ti)) aspects[aspectName] = [`${Math.max(n-1,1)} Chair${n>2?'s':''}, Table`];
            else if (/bedroom/i.test(_ti)) aspects[aspectName] = ['Bed Frame, Nightstand, Dresser'];
            else if (/sofa|sectional|couch/i.test(_ti)) aspects[aspectName] = ['Sofa'];
            else aspects[aspectName] = [`${n} Piece Set`];
          } else if (aspectName === 'Number of Items in Set' || aspectName === 'Number of Pieces') {
            const pm2 = (product.title||'').match(/(\d+)\s*(?:piece|pc|pack)/i);
            aspects[aspectName] = [pm2 ? pm2[1] : '1'];
          } else if (aspectName === 'Room') {
            const _ti2 = (product.title||'').toLowerCase();
            if (/dining/i.test(_ti2)) aspects[aspectName] = ['Dining Room'];
            else if (/bedroom/i.test(_ti2)) aspects[aspectName] = ['Bedroom'];
            else if (/living/i.test(_ti2)) aspects[aspectName] = ['Living Room'];
            else if (/office/i.test(_ti2)) aspects[aspectName] = ['Office'];
            else if (/outdoor|patio/i.test(_ti2)) aspects[aspectName] = ['Patio/Garden'];
            else if (/kitchen/i.test(_ti2)) aspects[aspectName] = ['Kitchen'];
            else aspects[aspectName] = ['Any Room'];
          } else if (aspectName === 'Assembly Required') {
            aspects[aspectName] = ['Yes'];
          } else if (aspectName === 'Shape') {
            const _ti3 = (product.title||'').toLowerCase();
            aspects[aspectName] = /round/i.test(_ti3) ? ['Round'] : /oval/i.test(_ti3) ? ['Oval'] : ['Rectangular'];
          } else if (aspectName === 'Seating Capacity') {
            const pm3 = (product.title||'').match(/(\d+)\s*(?:person|seat|chair)/i);
            aspects[aspectName] = [pm3 ? pm3[1] : '4'];
          } else if (aspectName === 'Firmness') {
            const _tf = (product.title||'').toLowerCase();
            if (/\bfirm\b/i.test(_tf)) aspects[aspectName] = ['Firm'];
            else if (/soft/i.test(_tf)) aspects[aspectName] = ['Soft'];
            else if (/plush/i.test(_tf)) aspects[aspectName] = ['Medium Plush'];
            else aspects[aspectName] = ['Medium'];
          } else if (aspectName === 'Type' && /mattress/i.test(product.title||'')) {
            const _tt = (product.title||'').toLowerCase();
            if (/memory.foam/i.test(_tt)) aspects[aspectName] = ['Memory Foam'];
            else if (/hybrid/i.test(_tt)) aspects[aspectName] = ['Hybrid'];
            else if (/innerspring/i.test(_tt)) aspects[aspectName] = ['Innerspring'];
            else aspects[aspectName] = ['Memory Foam'];
          } else if (aspectName === 'Features') {
            aspects[aspectName] = ['CertiPUR-US Certified'];
          } else if (aspectName === 'Thickness') {
            const thM = (product.title||'').match(/(\d{1,2})\s*(?:inch|in\.?|")/i);
            aspects[aspectName] = thM ? [thM[1] + ' in'] : ['10 in'];
          } else if (aspectName === 'Weight') {
            const wM2 = (product.title||'').match(/(\d+)\s*(?:lb|pound|kg)/i);
            aspects[aspectName] = wM2 ? [wM2[1] + ' lb'] : ['See Description'];
          } else if (aspectName === 'Package Quantity' || aspectName === 'Item Package Quantity' || aspectName === 'Number of Items') {
            const qM = (product.title||'').match(/(\d+)\s*(?:pack|bundle|piece|count|ct\b)/i);
            aspects[aspectName] = [qM ? qM[1] : '1'];
          } else if (aspectName === 'Unit Count') {
            const ucM = (product.title||'').match(/(\d+)\s*(?:pack|bundle|count|ct\b)/i);
            aspects[aspectName] = [ucM ? ucM[1] : '1'];
          } else if (aspectName === 'Size') {
            aspects[aspectName] = ['One Size'];
          } else if (aspectName === 'Material') {
            const _mt = (product.title||'').toLowerCase();
            if (/stainless.steel|steel/i.test(_mt)) aspects[aspectName] = ['Stainless Steel'];
            else if (/aluminum|aluminium/i.test(_mt)) aspects[aspectName] = ['Aluminum'];
            else if (/plastic|acrylic/i.test(_mt)) aspects[aspectName] = ['Plastic'];
            else if (/cotton/i.test(_mt)) aspects[aspectName] = ['Cotton'];
            else if (/polyester/i.test(_mt)) aspects[aspectName] = ['Polyester'];
            else if (/nylon/i.test(_mt)) aspects[aspectName] = ['Nylon'];
            else if (/silicone/i.test(_mt)) aspects[aspectName] = ['Silicone'];
            else if (/rubber/i.test(_mt)) aspects[aspectName] = ['Rubber'];
            else if (/wood|bamboo/i.test(_mt)) aspects[aspectName] = ['Wood'];
            else if (/glass/i.test(_mt)) aspects[aspectName] = ['Glass'];
            else if (/foam/i.test(_mt)) aspects[aspectName] = ['Foam'];
            else if (/mesh/i.test(_mt)) aspects[aspectName] = ['Mesh'];
            else aspects[aspectName] = ['Synthetic'];
          } else if (/Item\s*(Width|Height|Length|Depth|Diameter|Weight)/i.test(aspectName)) {
            // Extract dimensions from title if available
            const dimM = (product.title||'').match(/(\d+(?:\.\d+)?)\s*(?:x\s*\d+(?:\.\d+)?\s*x\s*\d+(?:\.\d+)?)?\s*(?:inch|in\.|"|cm|mm|ft)/i);
            aspects[aspectName] = [dimM ? dimM[1] + '"' : 'See Description'];
          } else if (/Item\s*Weight/i.test(aspectName)) {
            const wM = (product.title||'').match(/(\d+(?:\.\d+)?)\s*(?:lb|lbs|oz|kg|g)/i);
            aspects[aspectName] = [wM ? wM[0] : 'See Description'];
          } else if (/Number.*in.*Set|Pieces|Count|Quantity/i.test(aspectName)) {
            const nM = (product.title||'').match(/(\d+)\s*(?:piece|pack|set|count|pcs)/i);
            aspects[aspectName] = [nM ? nM[1] : '1'];
          } else if (/Room/i.test(aspectName)) {
            aspects[aspectName] = ['Any Room'];
          } else if (/Theme|Subject|Pattern|Design/i.test(aspectName)) {
            aspects[aspectName] = ['Modern'];
          } else if (/Mount|Mounting/i.test(aspectName)) {
            aspects[aspectName] = ['Wall Mount'];
          } else if (/Features/i.test(aspectName)) {
            aspects[aspectName] = ['See Description'];
          } else if (/Set.*includes|Includes/i.test(aspectName)) {
            aspects[aspectName] = ['See Description'];
          } else if (/^Item (Width|Height|Length|Depth|Diameter|Weight)$/i.test(aspectName)) {
            // Extract numeric dimension from title — eBay requires numeric format like "10 in"
            const _dimT = product.title || '';
            // Try dimension patterns: "18x12x5 inch", "18 inch", "18""
            const dimXM = _dimT.match(/(\d+(?:\.\d+)?)\s*x\s*(\d+(?:\.\d+)?)\s*x\s*(\d+(?:\.\d+)?)\s*(?:in|inch|\")/i);
            const dimSM = _dimT.match(/(\d+(?:\.\d+)?)\s*(?:inch|in\b|\")/i);
            const isW = /Width/i.test(aspectName), isH = /Height/i.test(aspectName);
            if (dimXM) {
              // "LxWxH" — assign appropriate dimension
              aspects[aspectName] = [isH ? (dimXM[3]+' in') : isW ? (dimXM[2]+' in') : (dimXM[1]+' in')];
            } else if (dimSM) {
              aspects[aspectName] = [dimSM[1] + ' in'];
            } else {
              // Sensible defaults by aspect name for common products
              const _defDims = { Width:'12 in', Length:'18 in', Height:'5 in', Depth:'12 in', Diameter:'10 in', Weight:'2 lb' };
              const _ak = Object.keys(_defDims).find(k => aspectName.includes(k)) || 'Length';
              aspects[aspectName] = [_defDims[_ak]];
            }
          } else if (/^Volume$/i.test(aspectName)) {
            const volM = (product.title||'').match(/([\d.]+)\s*(?:oz|fl oz|ml|liter|l\b|gal|gallon|cup)/i);
            aspects[aspectName] = [volM ? volM[0] : 'See Description'];
          } else if (/^Product$/i.test(aspectName)) {
            aspects[aspectName] = [(product.title||'').slice(0,65)];
          } else if (/^Department$/i.test(aspectName)) {
            const _td = (product.title||'').toLowerCase();
            if (/\bmen\b|\bmale\b/i.test(_td)) aspects[aspectName] = ['Men'];
            else if (/\bwomen\b|\bfemale\b|\bladies\b/i.test(_td)) aspects[aspectName] = ['Women'];
            else aspects[aspectName] = ['Unisex Adults'];
          } else if (/Dress\s*Length|Dress\s*Style/i.test(aspectName)) {
            const _tdr = (product.title||'').toLowerCase();
            if (/mini|short/i.test(_tdr)) aspects[aspectName] = aspectName.includes('Style') ? ['Mini'] : ['Mini (Up to 25\")'];
            else if (/midi|mid/i.test(_tdr)) aspects[aspectName] = aspectName.includes('Style') ? ['Midi'] : ['Midi (25-35\")'];
            else if (/maxi|long|floor/i.test(_tdr)) aspects[aspectName] = aspectName.includes('Style') ? ['Maxi'] : ['Maxi (35\"+)'];
            else aspects[aspectName] = aspectName.includes('Style') ? ['Midi'] : ['Midi (25-35\")'];
          } else if (/^Neckline$/i.test(aspectName)) {
            const _tn = (product.title||'').toLowerCase();
            if (/v.neck/i.test(_tn)) aspects[aspectName] = ['V-Neck'];
            else if (/crew|round.neck/i.test(_tn)) aspects[aspectName] = ['Crew Neck'];
            else if (/mock|turtle/i.test(_tn)) aspects[aspectName] = ['Mock/Turtleneck'];
            else if (/square/i.test(_tn)) aspects[aspectName] = ['Square'];
            else if (/scoop/i.test(_tn)) aspects[aspectName] = ['Scoop Neck'];
            else if (/off.shoulder/i.test(_tn)) aspects[aspectName] = ['Off Shoulder'];
            else aspects[aspectName] = ['Round Neck'];
          } else if (/^Sleeve\s*(Length|Type|Style)$/i.test(aspectName)) {
            const _ts = (product.title||'').toLowerCase();
            if (/sleeveless|tank|no.sleeve/i.test(_ts)) aspects[aspectName] = ['Sleeveless'];
            else if (/short.sleeve|3\/4/i.test(_ts)) aspects[aspectName] = ['Short Sleeve'];
            else if (/long.sleeve/i.test(_ts)) aspects[aspectName] = ['Long Sleeve'];
            else aspects[aspectName] = ['Short Sleeve'];
          } else if (/^Occasion$/i.test(aspectName)) {
            const _to = (product.title||'').toLowerCase();
            if (/casual|everyday/i.test(_to)) aspects[aspectName] = ['Casual'];
            else if (/formal|evening|gala/i.test(_to)) aspects[aspectName] = ['Formal'];
            else if (/work|office|business/i.test(_to)) aspects[aspectName] = ['Business'];
            else if (/beach|swim|summer/i.test(_to)) aspects[aspectName] = ['Beach/Resort'];
            else aspects[aspectName] = ['Casual'];
          } else if (/^Type$/i.test(aspectName) && /\bdress\b/i.test(product.title||'')) {
            const _tyt = (product.title||'').toLowerCase();
            if (/maxi/i.test(_tyt)) aspects[aspectName] = ['Maxi Dress'];
            else if (/midi/i.test(_tyt)) aspects[aspectName] = ['Midi Dress'];
            else if (/mini|shirt.dress|shirtdress/i.test(_tyt)) aspects[aspectName] = ['Shirt Dress'];
            else if (/wrap/i.test(_tyt)) aspects[aspectName] = ['Wrap Dress'];
            else aspects[aspectName] = ['Casual Dress'];
          } else if (/^(Season|Lining|Fit|Closure|Collar Style|Care Instructions|Fabric Type)$/i.test(aspectName)) {
            const _sea = aspectName.toLowerCase();
            if (_sea === 'season') aspects[aspectName] = ['Summer'];
            else if (_sea === 'lining') aspects[aspectName] = ['Unlined'];
            else if (_sea === 'fit') aspects[aspectName] = ['Regular'];
            else if (_sea === 'closure') aspects[aspectName] = ['Pullover'];
            else if (_sea === 'collar style') aspects[aspectName] = ['Round Collar'];
            else if (_sea === 'care instructions') aspects[aspectName] = ['Machine Wash'];
            else if (_sea === 'fabric type') aspects[aspectName] = ['Polyester'];
            else aspects[aspectName] = ['See Description'];
          } else {
            aspects[aspectName] = ['See Description'];
          }
        }
      }
    }
    if (missingAspects.length) {
      console.log(`[push] auto-filled missing aspects: ${missingAspects.join(', ')}`);
      // Must re-PUT group AND inventory items with the updated aspects — eBay checks both
      // Sync groupAspects with updated aspects
      for (const k of Object.keys(aspects)) {
        if (!groupAspects[k]) groupAspects[k] = aspects[k]; // add newly filled aspects
      }
      // Re-PUT the group
      try {
        const _gr2 = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`,
          { method: 'PUT', headers: auth, body: _buildGroupBody() }
        );
        if (_gr2.ok || _gr2.status === 204) console.log('[push] group re-PUT ok after auto-fill');
        else console.warn('[push] group re-PUT failed:', _gr2.status);
      } catch(e) { console.warn('[push] group re-PUT error:', e.message); }
      // Re-PUT inventory items with new aspects
      const _allSkus = variants.slice(0, 100);
      for (let _bi = 0; _bi < _allSkus.length; _bi += 25) {
        await fetch(`${EBAY_API}/sell/inventory/v1/bulk_create_or_replace_inventory_item`, {
          method: 'POST', headers: auth,
          body: JSON.stringify({ requests: _allSkus.slice(_bi, _bi + 25).map(v => ({
            sku: v.sku,
            inventoryItem: { condition: 'NEW', product: { title: listingTitle, imageUrls: [v.image].filter(Boolean), aspects: { ...groupAspects, ...v.dims } }, availability: { shipToLocationAvailability: { quantity: parseInt(v.qty)||0 } }, pricingSummary: { price: { value: v.price, currency: 'USD' } } }
          })) })
        }).catch(() => {});
        if (_bi + 25 < _allSkus.length) await sleep(150);
      }
    }
    if (attempt < 5) await sleep([0, 1500, 3000, 5000, 8000][attempt] || 2000);
  }
  return res.status(400).json({ error: 'Publish failed after 5 attempts', unrecoverable: true });
}

// ─── REVISE action ─────────────────────────────────────────────────────────────


// ── handlePromote: add published listing to 2% Promoted Listings Standard campaign ──────
async function handlePromote(body, res) {
  const { access_token, listingId } = body;
  if (!access_token || !listingId) return res.status(400).json({ error: 'Missing access_token or listingId' });

  const EBAY_API = getEbayUrls().EBAY_API;
  const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json' };
  const AD_RATE = '2.0';
  const CAMPAIGN_NAME = 'DropSync Auto Promote';

  try {
    // 1. Find or create the DropSync PLS campaign
    let campaignId = null;
    const listR = await fetch(`${EBAY_API}/sell/marketing/v1/ad_campaign?limit=50`, { headers: auth });
    if (listR.status === 403 || listR.status === 401) {
      console.warn('[promote] Marketing API not authorized — user needs to reconnect eBay');
      return res.json({ success: false, skipped: true, reason: 'reconnect' });
    }
    if (listR.ok) {
      const listD = await listR.json();
      const existing = (listD.campaigns || []).find(c =>
        c.campaignName === CAMPAIGN_NAME && c.campaignStatus !== 'ENDED' && c.campaignStatus !== 'ARCHIVED'
      );
      if (existing) campaignId = existing.campaignId;
    }

    // Create campaign if none found
    if (!campaignId) {
      const today = new Date().toISOString().split('T')[0];
      const cr = await fetch(`${EBAY_API}/sell/marketing/v1/ad_campaign`, {
        method: 'POST', headers: auth,
        body: JSON.stringify({
          campaignName:    CAMPAIGN_NAME,
          startDate:       new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'), // full ISO required
          fundingStrategy: { bidPercentage: AD_RATE, fundingModel: 'COST_PER_SALE' },
          marketplaceId:   'EBAY_US',
          // No campaignType field — determined by fundingModel (COST_PER_SALE = general/standard)
        }),
      });
      const loc = cr.headers?.get('Location') || '';
      const cd  = await cr.json().catch(() => ({}));
      campaignId = loc.split('/').pop() || cd.campaignId;
      if (!campaignId) { console.warn('[promote] campaign create failed:', JSON.stringify(cd).slice(0,150)); return res.json({ success: false, error: 'Campaign creation failed' }); }
      console.log(`[promote] created campaign ${campaignId}`);
    }

    // 2. Add listing to campaign
    // CPS general campaign: create single ad by listing ID
    // Correct endpoint: POST /ad_campaign/{id}/ad (not /ads/create_ads_by_listing_id)
    const adR = await fetch(`${EBAY_API}/sell/marketing/v1/ad_campaign/${campaignId}/ad`, {
      method: 'POST', headers: auth,
      body: JSON.stringify({ listingId, bidPercentage: AD_RATE }),
    });
    const adD = await adR.json().catch(() => ({}));
    const errors = (adD.errors || []).filter(e => e.severity === 'ERROR');
    if (adR.ok || adR.status === 207) {
      if (!errors.length) {
        console.log(`[promote] listing ${listingId} → campaign ${campaignId} @ ${AD_RATE}%`);
        return res.json({ success: true, campaignId, listingId, adRate: AD_RATE });
      }
    }
    console.warn(`[promote] HTTP ${adR.status}:`, JSON.stringify(adD).slice(0,200));
    return res.json({ success: false, error: (errors[0] || adD.errors?.[0])?.message || `HTTP ${adR.status}` });
  } catch (e) {
    console.error('[promote] error:', e.message);
    return res.json({ success: false, error: e.message });
  }
}


// ── handleRebuildPhotos ────────────────────────────────────────────────────────
// Flow:
//   1. GET inventory_item_group → all variant SKUs
//   2. Clear existing imageUrls on ALL inventory items
//   3. For each SKU: look up ASIN via skuToAsin
//   4. For each unique ASIN: use stored asinImages OR fetch fresh from Amazon
//   5. Batch PUT inventory items with correct images
//   6. PUT group with aspectsImageVariesBy = [chosenDim]
async function handleRebuildPhotos(body, res) {
  const { access_token, ebaySku, dimIndex = 0, variationImages = {},
          variations = [], product = {}, skuToAsin = {}, asinImages = {} } = body;
  if (!access_token || !ebaySku) return res.status(400).json({ error: 'Missing access_token or ebaySku' });

  const EBAY_API = getEbayUrls().EBAY_API;
  const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US', 'Content-Language': 'en-US' };

  // ── 1. Determine chosen dim ──────────────────────────────────────────────────
  const _allDimNames = [];
  if (product?._primaryDimName)   _allDimNames.push(product._primaryDimName);
  if (product?._secondaryDimName) _allDimNames.push(product._secondaryDimName);
  for (const ed of (product?._extraDimNames || [])) _allDimNames.push(ed);
  if (_allDimNames.length === 0)
    for (const v of (variations || [])) if (v.name && !_allDimNames.includes(v.name)) _allDimNames.push(v.name);
  if (_allDimNames.length === 0) return res.json({ success: false, error: 'No dimension names found on product' });

  const chosenDimName = _allDimNames[dimIndex % _allDimNames.length];
  const totalDims     = _allDimNames.length;
  console.log(`[rebuild-photos] dim="${chosenDimName}" (${_allDimNames.join(', ')})`);

  // ── 2. Get all variant SKUs from inventory_item_group ────────────────────────
  const groupSku = encodeURIComponent(ebaySku);
  let allSkus = [];
  try {
    const grpR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${groupSku}`, { headers: auth });
    if (grpR.ok) {
      const grpD = await grpR.json();
      allSkus = (grpD.variantSKUs || []).filter(Boolean);
    }
  } catch(e) {}
  if (!allSkus.length) return res.json({ success: false, error: 'No variant SKUs found for this listing' });
  console.log(`[rebuild-photos] ${allSkus.length} variant SKUs`);

  // ── 3. Batch GET all inventory items ─────────────────────────────────────────
  const inventoryItems = {};
  for (let i = 0; i < allSkus.length; i += 25) {
    const batch = allSkus.slice(i, i + 25);
    try {
      const bgr = await fetch(`${EBAY_API}/sell/inventory/v1/bulk_get_inventory_item`, {
        method: 'POST', headers: auth,
        body: JSON.stringify({ requests: batch.map(s => ({ sku: s })) }),
      });
      const bgd = await bgr.json().catch(() => ({}));
      for (const resp of (bgd.responses || []))
        if (resp.sku && resp.inventoryItem) inventoryItems[resp.sku] = resp.inventoryItem;
    } catch(e) {}
    if (i + 25 < allSkus.length) await sleep(200);
  }
  console.log(`[rebuild-photos] fetched ${Object.keys(inventoryItems).length}/${allSkus.length} inventory items`);

  // ── 4. Build ASIN → images map + derive skuToAsin via multi-strategy reconstruction ────
  // Strategies tried in order for every variant SKU that doesn't already have a direct hit:
  //   A. Direct lookup in passed-in skuToAsin
  //   B. Aspect ASIN field on the inventory item
  //   C. comboAsin matching (normalized substring)
  //   D. Dim-value slug reconstruction from product._variationValues (fallback for old
  //      truncated/hashed SKUs whose keys don't match the current 50-char format)
  const _effectiveSkuToAsin = { ...skuToAsin };

  // Build "_needsRecon" set of SKUs that aren't covered by strategy A yet. This is the
  // key change — the old code gated fallbacks on `< allSkus.length` which was always
  // false when the frontend passed a full-but-wrong-format skuToAsin.
  const _needsRecon = () => allSkus.filter(s => !_effectiveSkuToAsin[s]);

  // Strategy B: aspect ASIN (rarely present — DropSync strips ASIN at push time, but some
  // older listings kept it)
  let _derivedFromAspects = 0;
  for (const sku of _needsRecon()) {
    const item = inventoryItems[sku];
    const aspectAsin = item?.product?.aspects?.ASIN?.[0]
                    || item?.product?.aspects?.['Item Model Number']?.[0]
                    || item?.product?.mpn;
    if (aspectAsin && /^[A-Z0-9]{10}$/.test(aspectAsin)) {
      _effectiveSkuToAsin[sku] = aspectAsin;
      _derivedFromAspects++;
    }
  }
  if (_derivedFromAspects > 0) console.log(`[rebuild-photos] strategy B: ${_derivedFromAspects} SKUs resolved from aspects`);

  // Strategy C: comboAsin matching. Sort keys by length desc so the most specific
  // combination matches first (avoids "Red" winning over "Dark Red" via substring).
  const _comboAsinBody = body.comboAsin || {};
  const _sortedComboKeys = Object.entries(_comboAsinBody)
    .filter(([, asin]) => asin)
    .map(([key, asin]) => {
      const parts = key.split('|').map(p => p.toLowerCase().replace(/[^a-z0-9]/g, '')).filter(Boolean);
      return { key, asin, parts, totalLen: parts.reduce((s, p) => s + p.length, 0) };
    })
    .sort((a, b) => b.totalLen - a.totalLen);

  let _derivedFromCombo = 0;
  for (const sku of _needsRecon()) {
    const prefix = ebaySku + '-';
    const suffix = sku.startsWith(prefix) ? sku.slice(prefix.length) : sku;
    const normSuffix = suffix.toLowerCase().replace(/[^a-z0-9]/g, '');
    for (const { asin, parts } of _sortedComboKeys) {
      if (parts.length && parts.every(p => normSuffix.includes(p))) {
        _effectiveSkuToAsin[sku] = asin;
        _derivedFromCombo++;
        break;
      }
    }
  }
  if (_derivedFromCombo > 0) console.log(`[rebuild-photos] strategy C: ${_derivedFromCombo} SKUs resolved from comboAsin`);

  // Strategy D: dim-value slug reconstruction — same logic as smartSync. Works even
  // when variant SKUs are truncated/hashed because we match dim-value slugs against
  // the SKU text rather than expecting an exact key.
  const _stillUncovered = _needsRecon();
  if (_stillUncovered.length > 0 && Object.keys(_comboAsinBody).length > 0) {
    const _slug = s => (s||'').replace(/[^A-Z0-9]/gi,'_').toUpperCase().replace(/_+/g,'_').replace(/^_|_$/g,'');
    // Build slug → ASIN from comboAsin keys (comboAsin keys are "PrimVal|SecVal")
    const slugToAsin = {};
    for (const [comboKey, asin] of Object.entries(_comboAsinBody)) {
      if (!asin) continue;
      const parts = comboKey.split('|').filter(Boolean);
      if (!parts.length) continue;
      const fullSlug = parts.map(_slug).join('_');
      if (fullSlug) slugToAsin[fullSlug] = asin;
      // Primary-only fallback for SKUs that truncated/hashed the secondary dim
      if (parts.length > 1) {
        const primary = _slug(parts[0]);
        if (primary && !slugToAsin[primary]) slugToAsin[primary] = asin;
      }
    }
    // CRITICAL: sort slugs by length DESC so "LAVENDER_BUTTERFLY" wins over "BUTTERFLY"
    const _sortedSlugs = Object.entries(slugToAsin).sort((a, b) => b[0].length - a[0].length);
    const _pfxUpper = ebaySku.toUpperCase() + '-';
    let _derivedFromSlugs = 0;
    for (const sku of _stillUncovered) {
      const sfx = (sku.toUpperCase().startsWith(_pfxUpper)
        ? sku.slice(ebaySku.length + 1)
        : sku).toUpperCase();
      for (const [slug, asin] of _sortedSlugs) {
        if (sfx.startsWith(slug) || sfx.includes(slug)) {
          _effectiveSkuToAsin[sku] = asin;
          _derivedFromSlugs++;
          break;
        }
      }
    }
    if (_derivedFromSlugs > 0) console.log(`[rebuild-photos] strategy D: ${_derivedFromSlugs} SKUs resolved from slug reconstruction`);
  }

  // Strategy E: aspect-based matching — the strongest fallback for truncated/hashed SKUs.
  // Reads the Color/Size/Fit_type/etc. aspects directly from each SKU's inventory item
  // and finds a comboAsin entry whose compound key contains ALL the aspect values. This
  // works regardless of how the SKU string is formatted because we never parse the SKU
  // text — we rely on eBay's structured aspect data which is always correctly keyed.
  const _stillUncoveredE = _needsRecon();
  if (_stillUncoveredE.length > 0 && Object.keys(_comboAsinBody).length > 0) {
    // Build: list of { asin, parts } where parts is the set of normalized sub-values
    // from the comboKey (splitting on both `|` and ` / `). Sorted by total length so
    // more-specific compound keys win over less-specific ones.
    const _comboParts = Object.entries(_comboAsinBody)
      .filter(([, asin]) => asin)
      .map(([key, asin]) => {
        const parts = key.split(/\s*\|\s*|\s*\/\s*/)
          .map(p => (p||'').toLowerCase().trim())
          .filter(Boolean);
        return { key, asin, parts, totalLen: parts.reduce((s,p) => s + p.length, 0) };
      })
      .sort((a, b) => b.totalLen - a.totalLen);

    const _norm = s => (s||'').toLowerCase().trim();
    const _normFuzzy = s => (s||'').toLowerCase().replace(/[^a-z0-9]/g, '');

    let _derivedFromAspectsE = 0;
    for (const sku of _stillUncoveredE) {
      const item = inventoryItems[sku];
      const aspects = item?.product?.aspects || {};
      // Collect all aspect values as a flat set (lowercase)
      const aspectVals = new Set();
      const aspectValsFuzzy = new Set();
      for (const v of Object.values(aspects)) {
        for (const val of (Array.isArray(v) ? v : [v])) {
          const n = _norm(val);
          if (n) aspectVals.add(n);
          const f = _normFuzzy(val);
          if (f) aspectValsFuzzy.add(f);
        }
      }
      if (aspectVals.size === 0) continue;

      // Find the comboAsin entry whose parts are ALL represented in aspectVals (direct
      // match first, fuzzy fallback for compound values like "3X-Large" vs "3XL").
      let best = null;
      for (const cp of _comboParts) {
        if (cp.parts.length === 0) continue;
        const allDirect = cp.parts.every(p => aspectVals.has(p));
        const allFuzzy  = !allDirect && cp.parts.every(p => aspectValsFuzzy.has(_normFuzzy(p)));
        if (allDirect || allFuzzy) { best = cp; break; }
      }
      if (best) {
        _effectiveSkuToAsin[sku] = best.asin;
        _derivedFromAspectsE++;
      }
    }
    if (_derivedFromAspectsE > 0) console.log(`[rebuild-photos] strategy E: ${_derivedFromAspectsE} SKUs resolved from inventory aspects`);
  }

  const _finalCovered = allSkus.filter(s => _effectiveSkuToAsin[s]).length;
  console.log(`[rebuild-photos] SKU→ASIN resolution: ${_finalCovered}/${allSkus.length} covered`);

  const _asinImgMap = { ...asinImages }; // asin → [urls]

  // Find unique ASINs we need
  const _neededAsins = new Set();
  for (const sku of allSkus) {
    const asin = _effectiveSkuToAsin[sku];
    if (asin && !_asinImgMap[asin]) _neededAsins.add(asin);
  }

  // Don't server-fetch Amazon here — datacenter IPs are blocked.
  // Browser should send fresh asinImages via doRebuildPhotos() which uses residential IP.
  // Log which ASINs are missing so user knows to re-try with browser relay active.
  if (_neededAsins.size > 0) {
    console.log(`[rebuild-photos] ${_neededAsins.size} ASINs have no stored images — browser should pre-fetch these. Tip: run Rebuild Photos with browser relay active to get residential IP images.`);
  }

  // ── 5. Build variationImages keyed by chosenDim from asinImages + skuToAsin ───
  // This lets the frontend update its stored product so future operations use the right dim
  const _newVarImages = {}; // { chosenDimName: { "Black": [url,...], "Navy": [...] } }
  const _comboAsin = body.comboAsin || {}; // { "FitType|Color / Size": asin, ... }

  // Collect the set of KNOWN values for the chosen dimension. We use these to
  // identify which part of a comboKey corresponds to chosenDim, instead of
  // relying on positional lookup (which is broken when comboKey ordering
  // doesn't match _allDimNames ordering — e.g. comboKey="FitType|Color / Size"
  // but _primaryDimName="Color" puts Color at index 0 and extracts "FitType").
  const _chosenDimVariation = (variations || []).find(v => v.name === chosenDimName);
  const _chosenDimValues = new Set((_chosenDimVariation?.values || []).map(v => (v.value || '').toLowerCase().trim()));
  // Also build a normalized lookup that ignores whitespace/punct for fuzzy matching
  const _chosenDimValuesNorm = new Map();
  for (const v of (_chosenDimVariation?.values || [])) {
    const raw = (v.value || '').trim();
    if (raw) _chosenDimValuesNorm.set(raw.toLowerCase().replace(/[^a-z0-9]/g, ''), raw);
  }

  // Helper: given a comboKey, find the sub-part that matches a known value of chosenDim
  const _extractDimValFromCombo = (comboKey) => {
    // Split the whole key by both `|` and ` / ` — gets all possible sub-values.
    // "Seamless|Grey / 3X-Large" → ["Seamless", "Grey", "3X-Large"]
    const rawParts = comboKey.split(/\s*\|\s*|\s*\/\s*/).map(s => s.trim()).filter(Boolean);
    // Direct match
    for (const part of rawParts) {
      if (_chosenDimValues.has(part.toLowerCase())) return part;
    }
    // Normalized fuzzy match (strips punctuation/whitespace)
    for (const part of rawParts) {
      const norm = part.toLowerCase().replace(/[^a-z0-9]/g, '');
      if (_chosenDimValuesNorm.has(norm)) return _chosenDimValuesNorm.get(norm);
    }
    return null;
  };

  let _comboMatched = 0, _comboUnmatched = 0;
  for (const [comboKey, asin] of Object.entries(_comboAsin || {})) {
    if (!asin || !_asinImgMap[asin]?.length) continue;
    const dimVal = _extractDimValFromCombo(comboKey);
    if (!dimVal) { _comboUnmatched++; continue; }
    if (!_newVarImages[chosenDimName]) _newVarImages[chosenDimName] = {};
    if (!_newVarImages[chosenDimName][dimVal]) {
      _newVarImages[chosenDimName][dimVal] = _asinImgMap[asin].slice(0, 6);
    }
    _comboMatched++;
  }
  // Also fill from skuToAsin + inventory-item aspects if comboAsin didn't cover
  let _aspectMatched = 0;
  for (const [sku, asin] of Object.entries(_effectiveSkuToAsin)) {
    if (!asin || !_asinImgMap[asin]?.length) continue;
    const item = inventoryItems[sku];
    const aspects = item?.product?.aspects || {};
    // Try exact dim name, then lowercase, then underscore variants
    const dimVal = (aspects[chosenDimName] || aspects[chosenDimName.toLowerCase()] || aspects[chosenDimName.replace(/_/g, ' ')] || [])[0];
    if (!dimVal) continue;
    if (!_newVarImages[chosenDimName]) _newVarImages[chosenDimName] = {};
    if (!_newVarImages[chosenDimName][dimVal]) {
      _newVarImages[chosenDimName][dimVal] = _asinImgMap[asin].slice(0, 6);
      _aspectMatched++;
    }
  }
  console.log(`[rebuild-photos] rebuilt variationImages[${chosenDimName}]: ${Object.keys(_newVarImages[chosenDimName] || {}).length} values (combo ${_comboMatched}/${Object.keys(_comboAsin).length} matched, ${_comboUnmatched} unmatched, +${_aspectMatched} from aspects)`);

  // ── 6. Build PUT requests: assign ASIN images (preserve existing if unresolved) ─────
  const requests = [];
  let noAsin = 0, noImages = 0, noItem = 0, preserved = 0;

  for (const sku of allSkus) {
    const item = inventoryItems[sku];
    // Surface SKUs whose inventory_item fetch failed — previously these were silently dropped,
    // making the endpoint report updated:0 with no explanation for the vanishing variants.
    if (!item) {
      noItem++;
      preserved++;
      console.warn(`[rebuild-photos] preserving ${sku.slice(-24)} — bulk_get_inventory_item had no data`);
      continue;
    }

    const asin = _effectiveSkuToAsin[sku];
    const imgs = asin ? (_asinImgMap[asin] || []) : [];

    // If we couldn't resolve the ASIN or don't have fresh images for it, SKIP the PUT
    // entirely rather than clearing the variant's existing images. The old code would
    // overwrite with an empty array, destroying good images whenever a reconstruction
    // gap left a SKU unmatched.
    if (!asin) {
      noAsin++;
      preserved++;
      console.warn(`[rebuild-photos] preserving ${sku.slice(-24)} — no ASIN resolved, keeping existing images`);
      continue;
    }
    if (!imgs.length) {
      noImages++;
      preserved++;
      console.warn(`[rebuild-photos] preserving ${sku.slice(-24)} — ASIN ${asin} has no fresh images, keeping existing`);
      continue;
    }

    requests.push({
      sku,
      // Whitelist only the fields that eBay accepts in bulk_create_or_replace_inventory_item.
      // The GET response includes extra fields (like a nested `sku`, `groupIds`, `locale`,
      // and others) that silently cause the PUT to fail with no error body — the responses
      // array comes back empty, making `updated:0 failed:0` look like success with nothing
      // happening. Only pass the documented writable fields.
      inventoryItem: {
        availability:          item.availability,
        condition:             item.condition,
        conditionDescription:  item.conditionDescription,
        conditionDescriptors:  item.conditionDescriptors,
        packageWeightAndSize:  item.packageWeightAndSize,
        product: {
          title:        item.product?.title,
          description:  item.product?.description,
          aspects:      item.product?.aspects,
          brand:        item.product?.brand,
          mpn:          item.product?.mpn,
          ean:          item.product?.ean,
          upc:          item.product?.upc,
          isbn:         item.product?.isbn,
          epid:         item.product?.epid,
          subtitle:     item.product?.subtitle,
          videoIds:     item.product?.videoIds,
          imageUrls:    imgs.slice(0, 12), // eBay max 12 images — fresh ones
        },
        locale:  item.locale,
      },
    });
  }
  console.log(`[rebuild-photos] updating ${requests.length} / preserving ${preserved} SKUs (noItem=${noItem}, noAsin=${noAsin}, noImages=${noImages})`);

  // ── 6. Batch PUT inventory items ─────────────────────────────────────────────
  let updated = 0, failed = 0;
  const _putErrors = []; // First few errors surfaced in the response for frontend diagnosis
  let _putHttpStatus = null;
  let _putHttpErrorBody = null;
  for (let i = 0; i < requests.length; i += 25) {
    const batch = requests.slice(i, i + 25);
    try {
      const putr = await fetch(`${EBAY_API}/sell/inventory/v1/bulk_create_or_replace_inventory_item`, {
        method: 'POST', headers: auth,
        body: JSON.stringify({ requests: batch }),
      });
      _putHttpStatus = putr.status;
      const putd = await putr.json().catch(() => ({}));
      // If eBay returns an HTTP error with no `responses` array, surface the body
      // so we can see exactly what it rejected. Previously we silently swallowed it
      // and reported `updated: 0` with no explanation.
      if (!putr.ok && !Array.isArray(putd.responses)) {
        _putHttpErrorBody = JSON.stringify(putd).slice(0, 800);
        console.warn(`[rebuild-photos] PUT bulk HTTP ${putr.status}: ${_putHttpErrorBody}`);
        failed += batch.length;
        if (_putErrors.length < 5) {
          _putErrors.push({ httpStatus: putr.status, body: _putHttpErrorBody });
        }
        continue;
      }
      for (const resp of (putd.responses || [])) {
        if ([200, 201, 204].includes(resp.statusCode)) updated++;
        else {
          failed++;
          const errSnippet = JSON.stringify(resp.errors || []).slice(0, 200);
          console.warn(`[rebuild-photos] PUT failed ${resp.sku}: statusCode=${resp.statusCode} ${errSnippet}`);
          if (_putErrors.length < 5) {
            _putErrors.push({
              sku:        (resp.sku || '').slice(-30),
              statusCode: resp.statusCode,
              errors:     (resp.errors || []).slice(0, 2).map(e => ({
                errorId:   e.errorId,
                message:   (e.message || '').slice(0, 200),
                parameters: (e.parameters || []).slice(0, 3),
              })),
            });
          }
        }
      }
    } catch(e) {
      console.warn('[rebuild-photos] PUT error:', e.message);
      if (_putErrors.length < 5) _putErrors.push({ exception: e.message });
    }
    if (i + 25 < requests.length) await sleep(300);
  }

  // ── 7. Update inventory_item_group: aspectsImageVariesBy + reorder specs ───────
  // Setting aspectsImageVariesBy tells eBay which dim drives image swatches.
  // Reordering specifications puts chosen dim first — eBay shows it as primary.
  let listingIdForPublish = body.ebayListingId || null;
  try {
    const grpR2 = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${groupSku}`, { headers: auth });
    if (grpR2.ok) {
      const grpD2 = await grpR2.json();
      if (!listingIdForPublish && grpD2.listing?.listingId) listingIdForPublish = grpD2.listing.listingId;
      grpD2.variesBy = grpD2.variesBy || {};

      // Set aspectsImageVariesBy = chosen dim
      grpD2.variesBy.aspectsImageVariesBy = [chosenDimName];

      // Reorder specifications so chosen dim is FIRST (eBay uses first as primary swatch)
      const specs = grpD2.variesBy.specifications || [];
      const chosenSpec = specs.find(s => s.name === chosenDimName);
      const otherSpecs = specs.filter(s => s.name !== chosenDimName);
      if (chosenSpec) {
        grpD2.variesBy.specifications = [chosenSpec, ...otherSpecs];
        console.log(`[rebuild-photos] reordered specs: ${[chosenSpec, ...otherSpecs].map(s => s.name).join(', ')}`);
      }

      const grpPut = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${groupSku}`, {
        method: 'PUT', headers: auth, body: JSON.stringify(grpD2),
      });
      if (grpPut.ok) {
        console.log(`[rebuild-photos] group updated — aspectsImageVariesBy=["${chosenDimName}"], specs reordered`);
      } else {
        const grpErr = await grpPut.json().catch(() => ({}));
        console.warn(`[rebuild-photos] group PUT ${grpPut.status}:`, JSON.stringify(grpErr).slice(0, 200));
      }
    }
  } catch(e) { console.warn('[rebuild-photos] group update error:', e.message); }

  // ── 8. Re-publish listing so eBay picks up the spec reordering ───────────────
  // eBay caches variation structure — need to re-publish to apply changes on the live listing
  if (listingIdForPublish) {
    try {
      // Find any offer ID to publish
      const _offerSku = allSkus[0];
      if (_offerSku) {
        const _ofr = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(_offerSku)}&limit=1`, { headers: auth });
        if (_ofr.ok) {
          const _ofd = await _ofr.json();
          const _offerId = (_ofd.offers || [])[0]?.offerId;
          if (_offerId) {
            const _pubR = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${_offerId}/publish`, {
              method: 'POST', headers: auth, body: JSON.stringify({}),
            });
            const _pubD = await _pubR.json().catch(() => ({}));
            if (_pubD.listingId) {
              console.log(`[rebuild-photos] re-published listing ${_pubD.listingId} — variation structure updated`);
            } else {
              console.warn(`[rebuild-photos] publish response:`, JSON.stringify(_pubD).slice(0, 200));
            }
          }
        }
      }
    } catch(e) { console.warn('[rebuild-photos] re-publish error:', e.message); }
  }

  console.log(`[rebuild-photos] done — updated=${updated} failed=${failed} noAsin=${noAsin} noImages=${noImages} noItem=${noItem} allSkus=${allSkus.length}${_putHttpStatus ? ` putHttp=${_putHttpStatus}` : ''}`);
  return res.json({
    success: true,
    chosenDim:       chosenDimName,
    dimIndex:        dimIndex % totalDims,
    totalDims,
    nextDimIndex:    (dimIndex + 1) % totalDims,
    nextDim:         _allDimNames[(dimIndex + 1) % totalDims],
    updated, failed, noAsin, noImages, noItem,
    allSkusCount:    allSkus.length,
    inventoryFetched: Object.keys(inventoryItems).length,
    skuToAsinCovered: allSkus.filter(s => _effectiveSkuToAsin[s]).length,
    asinImagesProvided: Object.keys(_asinImgMap).length,
    comboMatched: _comboMatched,
    comboUnmatched: _comboUnmatched,
    requestsBuilt: requests.length,
    putHttpStatus: _putHttpStatus,
    putErrors:    _putErrors.length ? _putErrors : undefined,
    // Return new dim order + variationImages so frontend can update stored product
    newPrimaryDim:   chosenDimName,
    newSecondaryDim: _allDimNames.find(d => d !== chosenDimName) || null,
    newAllDims:      [chosenDimName, ..._allDimNames.filter(d => d !== chosenDimName)],
    variationImages: _newVarImages,
  });
}


module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || req.body?.action;
  const body   = req.body || {};

  // Store scraperApiKey globally when client sends it
  if (body.scraperApiKey && typeof body.scraperApiKey === 'string' && body.scraperApiKey.length > 8) {
    global._scraperApiKey = body.scraperApiKey;
    console.log('[scraper] API key received and stored');
  }

  try {

    // ── AUTH ──────────────────────────────────────────────────────────────────
    if (action === 'auth') {
      const E = getEbayUrls(); // production only
      const REDIRECT = E.REDIRECT || `${req.headers['x-forwarded-proto']||'https'}://${req.headers.host}/api/ebay?action=callback`;
      console.log(`[auth] production client_id=${E.CLIENT_ID?.slice(0,20)} redirect=${REDIRECT}`);
      const url = `${E.EBAY_AUTH}?client_id=${E.CLIENT_ID}&redirect_uri=${encodeURIComponent(REDIRECT)}&response_type=code&scope=${encodeURIComponent(SCOPES)}&state=production`;
      return res.json({ url });
    }

    if (action === 'callback') {
      const E = getEbayUrls(); // production only
      const REDIRECT = E.REDIRECT || `${req.headers['x-forwarded-proto']||'https'}://${req.headers.host}/api/ebay?action=callback`;
      const creds = Buffer.from(`${E.CLIENT_ID}:${E.CLIENT_SECRET}`).toString('base64');
      console.error(`[callback] production tok_url=${E.EBAY_TOK} client=${E.CLIENT_ID?.slice(0,25)} redirect=${REDIRECT}`);
      const r = await fetch(E.EBAY_TOK, {
        method: 'POST',
        headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `grant_type=authorization_code&code=${encodeURIComponent(req.query.code)}&redirect_uri=${encodeURIComponent(REDIRECT)}`,
      });
      const d = await r.json();
      if (!d.access_token) {
        console.error('[callback] eBay token exchange failed:', JSON.stringify(d).slice(0,300));
        return res.setHeader('Content-Type','text/html').send(
          `<!DOCTYPE html><html><body><p style="font-family:sans-serif;padding:40px;color:red">
            ❌ eBay auth failed: ${d.error || d.error_description || JSON.stringify(d).slice(0,200)}
          </p></body></html>`
        );
      }
      const payload = {
        type: 'ebay_auth',
        token: d.access_token,
        refresh: d.refresh_token,
        expiry: Date.now() + ((d.expires_in||7200)-120)*1000
      };
      return res.setHeader('Content-Type','text/html').send(
        `<!DOCTYPE html><html><body>
        <p style="font-family:sans-serif;padding:40px">✅ Connected to eBay! Closing…</p>
        <script>
          var payload = ${JSON.stringify(payload)};
          // Try postMessage to opener first
          if (window.opener) {
            window.opener.postMessage(payload, '*');
            setTimeout(function(){ window.close(); }, 800);
          } else {
            // Fallback: store in localStorage then redirect back to app
            try { localStorage.setItem('ebay_auth_pending', JSON.stringify(payload)); } catch(e){}
            setTimeout(function(){ window.location.href = '/'; }, 800);
          }
        </script>
        </body></html>`
      );
    }

    if (action === 'refresh') {
      const E = getEbayUrls(); // production only
      const creds = Buffer.from(`${E.CLIENT_ID}:${E.CLIENT_SECRET}`).toString('base64');
      const r = await fetch(E.EBAY_TOK, {
        method: 'POST',
        headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `grant_type=refresh_token&refresh_token=${encodeURIComponent(body.refresh_token)}&scope=${encodeURIComponent(SCOPES)}`,
      });
      const d = await r.json();
      if (!d.access_token) return res.status(400).json({ error: 'Token refresh failed', raw: d });
      return res.json({ access_token: d.access_token, expires_in: d.expires_in, expiry: Date.now() + ((d.expires_in||7200)-120)*1000 });
    }

    // ── TEST CREDS ───────────────────────────────────────────────────────────
    if (action === 'test_creds') {
      const E = getEbayUrls(); // production only
      // Try client credentials grant to verify client_id/secret work
      const creds = Buffer.from(`${E.CLIENT_ID}:${E.CLIENT_SECRET}`).toString('base64');
      const r = await fetch(E.EBAY_TOK, {
        method: 'POST',
        headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `grant_type=client_credentials&scope=${encodeURIComponent('https://api.ebay.com/oauth/api_scope')}`,
      });
      const d = await r.json();
      return res.json({ env: 'production', client_id: E.CLIENT_ID, has_secret: !!E.CLIENT_SECRET, result: d });
    }

    // ── POLICIES ─────────────────────────────────────────────────────────────
    if (action === 'policies') {
      const token = body.access_token || req.query.access_token;
      if (!token) return res.status(400).json({ error: 'No token' });
      const EBAY_API = getEbayUrls().EBAY_API; // production only
      const auth = { Authorization: `Bearer ${token}`, 'Accept-Language': 'en-US' };
      const [fp, pp, rp] = await Promise.all([
        fetch(`${EBAY_API}/sell/account/v1/fulfillment_policy?marketplace_id=EBAY_US`, { headers: auth }).then(r => r.json()),
        fetch(`${EBAY_API}/sell/account/v1/payment_policy?marketplace_id=EBAY_US`,     { headers: auth }).then(r => r.json()),
        fetch(`${EBAY_API}/sell/account/v1/return_policy?marketplace_id=EBAY_US`,      { headers: auth }).then(r => r.json()),
      ]);
      return res.json({
        fulfillment: (fp.fulfillmentPolicies||[]).map(p => ({ id: p.fulfillmentPolicyId, name: p.name })),
        payment:     (pp.paymentPolicies||[]).map(p => ({ id: p.paymentPolicyId, name: p.name })),
        return:      (rp.returnPolicies||[]).map(p => ({ id: p.returnPolicyId, name: p.name })),
      });
    }

    // ── SCRAPE: Amazon → structured product data ──────────────────────────────
    if (action === 'scrape') {
      // Single unified scraper — delegates to scrapeAmazonProduct
      let url = (body.url || req.query.url || '').trim();
      if (!url) return res.status(400).json({ error: 'No URL provided' });

      // Normalize to clean dp/ASIN URL
      const asinM = url.match(/\/(?:dp|gp\/product)\/([A-Z0-9]{10})/);
      const asinP = url.match(/[?&]asin=([A-Z0-9]{10})/i);
      const asin  = asinM?.[1] || asinP?.[1];
      if (asin) url = `https://www.amazon.com/dp/${asin}?th=1`;

      // Use browser-provided HTML if available (bypasses datacenter-IP blocking)
      const clientHtml     = (body.pageHtml && body.pageHtml.length > 5000) ? body.pageHtml : null;
      const clientAsinData = body.clientAsinData || null;
      const primeOnly      = body.primeOnly === true;
      if (clientHtml) console.log(`[scrape] using browser HTML (${clientHtml.length} chars)`);
      if (primeOnly)  console.log(`[scrape] primeOnly mode ON — skipping non-Prime ASINs`);

      try {
        const product = await scrapeAmazonProduct(url, clientHtml, clientAsinData, primeOnly);
        if (!product || !product.title) {
          return res.json({ success: false, error: 'Amazon blocked the request (bot detection). Wait 30 seconds and try again, or paste the URL directly.' });
        }
        // Build colorAsinMap for worker
        const colorAsinMap = {};
        if (product.comboAsin) {
          for (const [key, asin] of Object.entries(product.comboAsin)) {
            const color = key.split('|')[0];
            if (color && asin && !colorAsinMap[color]) colorAsinMap[color] = asin;
          }
        }
        if (Object.keys(colorAsinMap).length) product.colorAsinMap = colorAsinMap;
        const colorGrp = product.variations?.find(v => /color/i.test(v.name));
        const pricesFound = Object.values(product.comboPrices || {}).filter(p => p > 0).length;
        const imagesFound = product.images?.length || 0;
        return res.json({ success: true, product, _debug: { pricesFound, imagesFound, totalColors: colorGrp?.values?.length || 0, colorAsinMapSize: Object.keys(colorAsinMap).length, pricesFailed: !!product._pricesFailed, pricesFailedReason: product._pricesFailedReason } });
      } catch(e) {
        console.error('[scrape] error:', e.message);
        return res.status(500).json({ success: false, error: e.message });
      }
    }

    // ── PUSH: create eBay listing ─────────────────────────────────────────────
    if (action === 'push') {
      return handlePush({ body, res, resolvePolicies, sanitizeTitle, ensureLocation, buildOffer, sleep, getEbayUrls });
    }

    // ── FETCH MY EBAY LISTINGS ────────────────────────────────────────────────
    if (action === 'fetchMyListings') {
      const { access_token } = body;
      if (!access_token) return res.status(400).json({ error: 'Missing access_token' });
      const EBAY_API = getEbayUrls().EBAY_API;
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };

      const allListings = [];
      const seenIds = new Set();

      // ── Strategy 1: Inventory API offers (catches DropSync-created listings) ──
      try {
        let offset = 0;
        const limit = 100;
        while (true) {
          const r = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=${limit}&offset=${offset}`, { headers: auth });
          const d = await r.json();
          if (!r.ok) { console.log('[fetchMyListings] inventory offers err:', d?.errors?.[0]?.message); break; }
          for (const o of (d.offers || [])) {
            if (!o.listingId || seenIds.has(o.listingId)) continue;
            seenIds.add(o.listingId);
            allListings.push({
              ebayListingId: o.listingId,
              ebaySku:       o.sku || '',
              title:         o.sku || '',
              price:         parseFloat(o.pricingSummary?.price?.value || 0),
              quantity:      o.availableQuantity || 0,
              image:         '',
              ebayUrl:       `https://www.ebay.com/itm/${o.listingId}`,
              aspects:       {},
            });
          }
          const total = d.total || 0;
          offset += limit;
          if (offset >= total || (d.offers||[]).length < limit) break;
        }
        console.log(`[fetchMyListings] inventory offers: ${allListings.length}`);
      } catch(e) { console.log('[fetchMyListings] inventory err:', e.message); }

      // ── Strategy 2: Trading API — single page, frontend paginates ─────────────
      const tradingPage = parseInt(body.page || 1);
      let tradingTotal = 1;
      try {
        const TRADING_API = 'https://api.ebay.com/ws/api.dll';
        const perPage = 50; // small pages = fast, no timeout
        const xmlBody = `<?xml version="1.0" encoding="utf-8"?><GetMyeBaySellingRequest xmlns="urn:ebay:apis:eBLBaseComponents"><RequesterCredentials><eBayAuthToken>${access_token}</eBayAuthToken></RequesterCredentials><ActiveList><Include>true</Include><Pagination><EntriesPerPage>${perPage}</EntriesPerPage><PageNumber>${tradingPage}</PageNumber></Pagination></ActiveList><ErrorLanguage>en_US</ErrorLanguage><WarningLevel>High</WarningLevel></GetMyeBaySellingRequest>`;
        const r = await fetch(TRADING_API, {
          method: 'POST',
          headers: {
            'Content-Type': 'text/xml',
            'X-EBAY-API-SITEID': '0',
            'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
            'X-EBAY-API-CALL-NAME': 'GetMyeBaySelling',
            'X-EBAY-API-IAF-TOKEN': access_token,
          },
          body: xmlBody,
        });
        const xml = await r.text();
        console.log(`[fetchMyListings] trading p${tradingPage} status=${r.status} len=${xml.length}`);
        if (r.ok && !xml.includes('<Ack>Failure</Ack>')) {
          const tpM = xml.match(/<TotalNumberOfPages>(\d+)<\/TotalNumberOfPages>/);
          if (tpM) tradingTotal = parseInt(tpM[1]);
          // Split-based parser — much faster than regex on large XML
          const parts = xml.split('<Item>');
          for (let pi = 1; pi < parts.length; pi++) {
            const endIdx = parts[pi].indexOf('</Item>');
            if (endIdx < 0) continue;
            const block = parts[pi].slice(0, endIdx);
            const g = tag => (block.match(new RegExp(`<${tag}(?:\\s[^>]*)?>([^<]*)<\/${tag}>`)) || [])[1]?.trim() || '';
            const itemId = g('ItemID');
            if (!itemId || seenIds.has(itemId)) continue;
            seenIds.add(itemId);
            const price = parseFloat(g('CurrentPrice') || g('BuyItNowPrice') || g('StartPrice') || '0') || 0;
            allListings.push({
              ebayListingId: itemId,
              ebaySku:  g('SKU'),
              title:    g('Title'),
              price,
              quantity: parseInt(g('QuantityAvailable') || g('Quantity') || '1'),
              image:    g('GalleryURL') || g('PictureURL') || '',
              ebayUrl:  `https://www.ebay.com/itm/${itemId}`,
              aspects:  {},
            });
          }
          console.log(`[fetchMyListings] p${tradingPage}/${tradingTotal}: ${allListings.length} total so far`);
        }
      } catch(e) { console.log('[fetchMyListings] trading err:', e.message); }

      // ── Enrich titles/images via inventory items for DropSync SKUs ────────────
      const dsItems = allListings.filter(l => l.ebaySku?.startsWith('DS-'));
      if (dsItems.length) {
        const skus = [...new Set(dsItems.map(l => l.ebaySku))].slice(0, 100);
        for (let i = 0; i < skus.length; i += 25) {
          try {
            const batch = skus.slice(i, i+25);
            const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item?sku=${batch.join('|')}`, { headers: auth });
            const id = await ir.json();
            for (const item of (id.inventoryItems || [])) {
              const l = allListings.find(x => x.ebaySku === item.sku);
              if (l) {
                if (!l.title && item.product?.title) l.title = item.product.title;
                if (!l.image && item.product?.imageUrls?.[0]) l.image = item.product.imageUrls[0];
              }
            }
          } catch {}
        }
      }

      console.log(`[fetchMyListings] TOTAL: ${allListings.length} (page ${body.page||1}/${tradingTotal||1})`);
      return res.json({ success: true, listings: allListings, total: allListings.length, tradingTotalPages: tradingTotal || 1, tradingPage: parseInt(body.page || 1) });
    }


    // ── FETCH MY LISTINGS DEBUG — returns raw XML ─────────────────────────────
    if (action === 'fetchMyListingsDebug') {
      const { access_token } = body;
      if (!access_token) return res.status(400).json({ error: 'Missing access_token' });
      const TRADING_API = 'https://api.ebay.com/ws/api.dll';
      const xmlBody = `<?xml version="1.0" encoding="utf-8"?>
<GetMyeBaySellingRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <RequesterCredentials><eBayAuthToken>${access_token}</eBayAuthToken></RequesterCredentials>
  <ActiveList><Include>true</Include><Pagination><EntriesPerPage>10</EntriesPerPage><PageNumber>1</PageNumber></Pagination></ActiveList>
  <ErrorLanguage>en_US</ErrorLanguage><WarningLevel>High</WarningLevel>
</GetMyeBaySellingRequest>`;
      const r = await fetch(TRADING_API, {
        method: 'POST',
        headers: {
          'Content-Type': 'text/xml',
          'X-EBAY-API-SITEID': '0',
          'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
          'X-EBAY-API-CALL-NAME': 'GetMyeBaySelling',
          'X-EBAY-API-IAF-TOKEN': access_token,
        },
        body: xmlBody,
      });
      const xmlText = await r.text();
      console.log('[fetchMyListingsDebug] status:', r.status, 'len:', xmlText.length);
      return res.json({ status: r.status, xml: xmlText.slice(0, 5000) });
    }



    // ── REVISE: update live listing images/price/stock from fresh Amazon scrape ─
    // Does NOT delete or re-publish — revises the inventory items in place
    if (action === 'revise') {
      // FIX: inject header into body so handleRevise doesn't need req in scope
      body._lastRevisedHeader = req.headers['x-last-revised'] || '0';
      return handleRevise(body, res);
    }

    // ── PROMOTE: add listing to 2% Promoted Listings Standard campaign ──────────
    if (action === 'promote') {
      return handlePromote(body, res);
    }

    // ── REBUILD PHOTOS: remap inventory item images by cycling dimension ─────────
    if (action === 'rebuild-photos') {
      return handleRebuildPhotos(body, res);
    }

    // ── WEBSTAURANTSTORE: scrape individual product page ──────────────────────
    if (action === 'ws-scrape') {
      const { url, html, wsSettings } = body;
      if (!url && !html) return res.status(400).json({ error: 'Missing url or html' });
      try {
        let pageHtml = html;
        if (!pageHtml && url) {
          const r = await fetch(url, {
            headers: {
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
              'Accept': 'text/html,application/xhtml+xml,*/*',
              'Accept-Language': 'en-US,en;q=0.9',
            }
          });
          if (!r.ok) return res.status(400).json({ error: `WS fetch failed: ${r.status}` });
          pageHtml = await r.text();
        }
        const product = wsScrapeProduct(pageHtml, url || '', wsSettings || {});
        if (!product) return res.status(400).json({ error: 'Could not parse WebstaurantStore product data' });
        return res.json({ success: true, product });
      } catch(e) {
        return res.status(500).json({ error: e.message });
      }
    }

    // ── WEBSTAURANTSTORE: extract product list from category page ─────────────
    if (action === 'ws-category') {
      const { url, html, page = 1 } = body;
      if (!url && !html) return res.status(400).json({ error: 'Missing url or html' });
      try {
        let pageHtml = html;
        const fetchUrl = url + (url.includes('?') ? `&p=${page}` : `?p=${page}`);
        if (!pageHtml) {
          const r = await fetch(fetchUrl, {
            headers: {
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
              'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
              'Accept-Language': 'en-US,en;q=0.9',
              'Cache-Control': 'no-cache',
              'Sec-Fetch-Dest': 'document',
              'Sec-Fetch-Mode': 'navigate',
              'Sec-Fetch-Site': 'none',
              'Upgrade-Insecure-Requests': '1',
            }
          });
          if (!r.ok) return res.status(400).json({ error: `WS fetch failed: ${r.status} for ${fetchUrl}` });
          pageHtml = await r.text();
        }
        const listings = wsExtractListings(pageHtml);
        console.log(`[ws-category] ${fetchUrl} -> ${listings.length} listings`);
        return res.json({ success: true, listings, page, hasMore: listings.length >= 60 });
      } catch(e) {
        return res.status(500).json({ error: e.message });
      }
    }

    // ── REPLENISH: full scrape + revise for Orders tab linking ───────────────────
    // Same as revise but triggered from orders tab after linking an Amazon URL.
    // Does a fresh scrape, then runs handleRevise with the scraped product data.
    if (action === 'replenish') {
      const { access_token, ebaySku, sourceUrl, ebayListingId, quantity, markup, handlingCost, clientHtml } = body;
      if (!access_token || !ebaySku || !sourceUrl) {
        return res.status(400).json({ error: 'Missing access_token, ebaySku, or sourceUrl' });
      }
      console.log(`[replenish] scraping ${sourceUrl}`);
      let product = null;
      try {
        product = await scrapeAmazonProduct(sourceUrl, clientHtml || null, body.clientAsinData || null);
      } catch(e) {
        return res.status(400).json({ error: 'Scrape failed: ' + e.message });
      }
      if (!product?.title) {
        return res.status(400).json({ error: 'No product data — Amazon may be blocking' });
      }
      // Check if multi-ASIN listing (each color is a separate ASIN)
      const uniqueAsins = new Set(Object.values(product.comboAsin || {}));
      const totalCombos = Object.keys(product.comboAsin || {}).length;
      // Only block if color-keyed multi-ASIN (one ASIN per primary value, shared across sizes)
      // Fully-multi listings (one ASIN per combo) can be revised normally
      const primaryCount = product.variations?.find(v => /color|colour|style/i.test(v.name))?.values?.length || 1;
      const isColorKeyed = uniqueAsins.size <= primaryCount * 1.2 && uniqueAsins.size < totalCombos * 0.5;
      if (isColorKeyed && uniqueAsins.size > 5) {
        return res.status(400).json({
          success: false,
          multiAsinMismatch: true,
          message: `Color-keyed multi-ASIN listing (${uniqueAsins.size} ASINs, ${primaryCount} colors) — use Re-push instead`
        });
      }
      // Run revise with the fresh scraped product
      const reviseBody = {
        access_token,
        ebaySku,
        ebayListingId: ebayListingId || '',
        sourceUrl,
        markup: markup || 25,
        handlingCost: handlingCost || 2,
        quantity: quantity || 1,
        fallbackComboAsin:    product.comboAsin    || null,
        fallbackComboInStock: product.comboInStock || null,
        fallbackComboPrices:  product.comboPrices  || null,
        fallbackVariations:   product.variations   || null,
        fallbackVariationImages: product.variationImages || null,
        fallbackImages:       [...new Set(product.images || [])].slice(0, 12),
        fallbackPrimaryDimName:   product._primaryDimName   || null,
        fallbackSecondaryDimName: product._secondaryDimName || null,
        fallbackPrice:  product.price || 0,
        fallbackTitle:  product.title || '',
        fallbackInStock: product.inStock !== false,
      };
      // handleRevise returns via res.json — wrap to capture result
      const originalJson = res.json.bind(res);
      let reviseResult = null;
      res.json = (data) => { reviseResult = data; return originalJson(data); };
      await handleRevise(reviseBody, res);
      // If handleRevise sent response, add replenish-specific fields
      if (reviseResult?.success) {
        // Already sent — can't modify response
        console.log(`[replenish] done: ${product.images?.length} images, ${Object.keys(product.variationImages||{}).length} var dims`);
      }
      return;
    }

    // ── SMART SYNC: lightweight price+qty update ──────────────────────────────────
    // 1. Scrape Amazon smartly (swatch prices from HTML, per-ASIN fetch only for gaps)
    // 2. Compute eBay price = applyMk(amazonPrice + amazonShipping)
    // 3. GET current eBay variant qty+price
    // ── UPDATE IMAGES: update group + per-variant imageUrls on eBay ─────────────
    // Separate from smartSync — only touches images, never price/qty
    if (action === 'updateImages') {
      const { access_token, ebaySku, images, variationImages, skuToAsin: _s2a } = body;
      if (!access_token || !ebaySku)
        return res.status(400).json({ error: 'Missing access_token or ebaySku' });

      const EBAY_API2 = getEbayUrls().EBAY_API;
      const auth2 = {
        Authorization: `Bearer ${access_token}`,
        'Content-Type': 'application/json',
        'Content-Language': 'en-US',
        'Accept-Language': 'en-US',
      };
      // Separate headers without Content-Language for GET requests
      const auth2get = {
        Authorization: `Bearer ${access_token}`,
        'Content-Type': 'application/json',
        'Accept-Language': 'en-US',
      };
      const normSku2  = ebaySku.trim();
      const mainImgs  = (images || []).filter(Boolean).slice(0, 12);
      const varImgs2  = variationImages || {}; // { dimName: { value: url } }
      const skuToAsin2 = _s2a || {};
      const results   = { groupOk: false, variantsOk: 0, variantsFail: 0, errors: [] };

      // 1. Get all variant SKUs from group
      const allVarSkus = new Set(Object.keys(skuToAsin2));
      try {
        const grpR2 = await fetch(`${EBAY_API2}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(normSku2)}`, { headers: auth2get });
        if (grpR2.ok) {
          const grpD2 = await grpR2.json().catch(() => ({}));
          for (const s of (grpD2.variantSKUs || [])) allVarSkus.add(s);

          // 2. Update group imageUrls if main images provided
          if (mainImgs.length > 0) {
            // Build group PUT from scratch — spreading grpD2 includes read-only fields
            const grpPut = {
              inventoryItemGroupKey: normSku2,
              variantSKUs:   grpD2.variantSKUs   || [],
              title:         grpD2.title         || '',
              description:   grpD2.description   || '',
              imageUrls:     mainImgs,
              aspects:       grpD2.aspects       || {},
              variesBy:      grpD2.variesBy      || {},
            };
            if (grpD2.subtitle)     grpPut.subtitle    = grpD2.subtitle;
            if (grpD2.videoIds)     grpPut.videoIds    = grpD2.videoIds;
            const grpPutR = await fetch(`${EBAY_API2}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(normSku2)}`, {
              method: 'PUT', headers: auth2, body: JSON.stringify(grpPut),
            });
            results.groupOk = grpPutR.ok || grpPutR.status === 204;
            if (!results.groupOk) {
              const grpErr = await grpPutR.text().catch(() => '');
              results.errors.push(`Group: ${grpPutR.status} ${grpErr.slice(0,100)}`);
              console.warn(`[updateImages] group PUT ${grpPutR.status}: ${grpErr.slice(0,150)}`);
            } else {
              console.log(`[updateImages] group imageUrls updated: ${mainImgs.length} images`);
            }
          }
        }
      } catch(_ge) {
        results.errors.push('Group fetch: ' + _ge.message);
      }

      // 3. Update each variant inventory item
      const skuList3 = [...allVarSkus];
      for (let _i3 = 0; _i3 < skuList3.length; _i3 += 6) {
        await Promise.all(skuList3.slice(_i3, _i3 + 6).map(async _vsku3 => {
          try {
            // GET current inventory item
            const _vr3 = await fetch(`${EBAY_API2}/sell/inventory/v1/inventory_item/${encodeURIComponent(_vsku3)}`, { headers: auth2get });
            if (!_vr3.ok) {
              results.errors.push(`GET ${_vsku3.slice(-18)}: ${_vr3.status}`);
              results.variantsFail++;
              return;
            }
            const _vd3 = await _vr3.json().catch(() => ({}));

            // Pick per-variant image — match SKU suffix against variationImages keys
            let _varUrls3 = mainImgs.slice(0, 12);
            if (Object.keys(varImgs2).length > 0) {
              const _sfx3 = _vsku3.toUpperCase().replace(normSku2.toUpperCase() + '-', '');
              outer: for (const [, _valMap3] of Object.entries(varImgs2)) {
                for (const [_val3, _url3] of Object.entries(_valMap3)) {
                  if (!_url3) continue;
                  const _slug3 = (_val3||'').replace(/[^A-Z0-9]/gi,'_').toUpperCase().replace(/_+/g,'_').replace(/^_|_$/g,'');
                  if (_sfx3.startsWith(_slug3)) {
                    _varUrls3 = [_url3, ..._varUrls3.filter(u => u !== _url3)].slice(0, 12);
                    break outer;
                  }
                }
              }
            }

            // Build minimal PUT body — only the fields eBay definitely accepts
            // conditionId / aspects / brand / packageWeightAndSize all trigger 2004 on some accounts
            const _curQty = _vd3.availability?.shipToLocationAvailability?.quantity ?? 1;
            const _putBody3 = {
              condition: _vd3.condition || 'NEW',
              product: {
                imageUrls: _varUrls3,
              },
              availability: { shipToLocationAvailability: { quantity: _curQty } },
            };
            // Only add title — safest optional field
            if (_vd3.product?.title) _putBody3.product.title = _vd3.product.title;

            const _pr3 = await fetch(`${EBAY_API2}/sell/inventory/v1/inventory_item/${encodeURIComponent(_vsku3)}`, {
              method: 'PUT', headers: auth2, body: JSON.stringify(_putBody3),
            });
            if (_pr3.ok || _pr3.status === 204) {
              results.variantsOk++;
            } else {
              const _pe3 = await _pr3.text().catch(() => '');
              results.errors.push(`PUT ${_vsku3.slice(-18)}: ${_pr3.status} ${_pe3.slice(0,80)}`);
              results.variantsFail++;
              console.warn(`[updateImages] variant PUT ${_vsku3.slice(-20)} ${_pr3.status}: ${_pe3.slice(0,120)}`);
            }
          } catch(_ve3) {
            results.variantsFail++;
            results.errors.push(_ve3.message);
          }
        }));
        if (_i3 + 6 < skuList3.length) await sleep(150);
      }
      console.log(`[updateImages] done — group=${results.groupOk} variants=${results.variantsOk}/${skuList3.length}`);

      // 4. Publish to make image changes live
      try {
        const pubR3 = await fetch(`${EBAY_API2}/sell/inventory/v1/offer/publish_by_inventory_item_group`, {
          method: 'POST', headers: auth2,
          body: JSON.stringify({ inventoryItemGroupKey: normSku2, marketplaceId: 'EBAY_US' }),
        });
        const pubD3 = await pubR3.json().catch(() => ({}));
        if (pubD3.listingId) {
          console.log(`[updateImages] published OK listingId=${pubD3.listingId}`);
        } else {
          const pubErr = (pubD3.errors||[]).map(e => e.errorId).join(',');
          console.warn(`[updateImages] publish ${pubR3.status} errors=${pubErr}`);
        }
      } catch(_pub3) {
        console.warn('[updateImages] publish error:', _pub3.message);
      }

      return res.json({
        success: results.variantsOk > 0 || results.groupOk,
        groupOk: results.groupOk,
        variantsOk: results.variantsOk,
        variantsFail: results.variantsFail,
        total: skuList3.length,
        errors: results.errors.slice(0, 5),
      });
    }

    // 4. PUT only what changed: qty 0→1 or 1→0, price if diff > $0.01
    // 5. Returns { success, synced, unchanged, priceChanges, stockChanges }
    // ── RESYNC FROM SCRAPE — full re-push to existing listing ────────────────
    // Architecture: fresh scrape → reuse existing groupSku → update inventory
    // items + offers → publish_by_inventory_item_group. Same listing ID, fresh
    // data top-to-bottom. Replaces the per-variant smartSync reconciliation
    // dance. Hard cap of 50 total variants on the listing — fresh variants
    // win, surviving zombies (existing eBay variants no longer on Amazon) get
    // forced qty=0, oldest zombies get evicted (offer DELETE) when over cap.
    if (action === 'resyncFromScrape') {
      const { access_token, ebaySku, sourceUrl,
              markup: mkRaw, handlingCost: handRaw, quantity: qtyRaw,
              fulfillmentPolicyId, paymentPolicyId, returnPolicyId } = body;
      if (!access_token || !ebaySku || !sourceUrl)
        return res.status(400).json({ error: 'Missing access_token, ebaySku, or sourceUrl' });

      const EBAY_API   = getEbayUrls().EBAY_API;
      const auth       = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Content-Language': 'en-US', 'Accept-Language': 'en-US' };
      const normSku    = ebaySku.trim().replace(/\s+/g, '');
      const HARD_CAP   = 50;
      console.log(`[resync] ${normSku} — ${sourceUrl}`);

      // STEP 1: get fresh product data
      // Prefer the worker-provided scrape (already done one step earlier in
      // worker.js → reviseProduct, passed in as fallback* fields). Only do a
      // fresh fetchPage when called from the frontend or some other context
      // that didn't pre-scrape.
      let product;
      if (body.comboAsin && Object.keys(body.comboAsin).length > 0 && body.fallbackComboPrices) {
        // Reconstitute a product object from the worker's scrape payload — same
        // shape scrapeAmazonProduct would have returned. No re-fetch needed.
        product = {
          _source: 'amazon',
          title: body.fallbackTitle || '',
          ebayTitle: body.fallbackTitle || '',
          description: body.fallbackDescription || body.fallbackTitle || '',
          price: parseFloat(body.fallbackPrice) || 0,
          images: body.fallbackImages || [],
          variations: body.fallbackVariations || [],
          variationImages: body.fallbackVariationImages || {},
          comboAsin: body.comboAsin,
          comboPrices: body.fallbackComboPrices,
          comboInStock: body.fallbackComboInStock || {},
          aspects: body.fallbackAspects || {},
          hasVariations: (body.fallbackVariations || []).length > 0,
          _primaryDimName: body.fallbackPrimaryDimName || null,
          _secondaryDimName: body.fallbackSecondaryDimName || null,
          shippingCost: parseFloat(body.fallbackShipping) || 0,
        };
        console.log(`[resync] using worker-provided scrape (${Object.keys(product.comboAsin).length} combos, ${Object.keys(product.comboPrices).length} prices)`);
      } else {
        const html = await fetchPage(sourceUrl, randUA()).catch(() => null);
        if (!html) {
          console.warn(`[resync] ${normSku} BLOCKED — Amazon parent unfetchable, aborting (no destructive changes)`);
          return res.json({ success: false, error: 'Amazon parent page blocked', skippable: true });
        }
        product = await scrapeAmazonProduct(sourceUrl, html, null);
        if (!product) return res.json({ success: false, error: 'Scrape returned null' });
        product._source = product._source || 'amazon';
      }

      // STEP 2: pull existing eBay group state
      const grpR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(normSku)}`, { headers: auth });
      if (!grpR.ok) return res.status(400).json({ error: `Group fetch failed: ${grpR.status}` });
      const grpData = await grpR.json().catch(() => ({}));
      const existingSkus = grpData.variantSKUs || [];
      console.log(`[resync] existing group has ${existingSkus.length} variant SKUs`);

      // STEP 3: discover offer IDs + lastModifiedDate per existing SKU
      const offerMap = {}; // sku → { offerId, lastModifiedDate, currentPrice }
      for (let i = 0; i < existingSkus.length; i += 5) {
        await Promise.all(existingSkus.slice(i, i + 5).map(async sku => {
          try {
            const r = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}&marketplace_id=EBAY_US`, { headers: auth });
            if (!r.ok) return;
            const d = await r.json().catch(() => ({}));
            const off = (d.offers || [])[0];
            if (off?.offerId) {
              offerMap[sku] = {
                offerId: off.offerId,
                lastModifiedDate: off.lastModifiedDate || off.listingStartDate || '',
                currentPrice: parseFloat(off.pricingSummary?.price?.value || 0),
              };
            }
          } catch(e) {}
        }));
        if (i + 5 < existingSkus.length) await sleep(120);
      }
      console.log(`[resync] discovered ${Object.keys(offerMap).length}/${existingSkus.length} offer IDs`);

      // STEP 4: build fresh variants via the same buildVariants the push uses
      const markupPct = parseFloat(mkRaw  ?? 23);
      const handling  = parseFloat(handRaw ?? 2);
      const defaultQty = parseInt(qtyRaw) || 1;
      const applyMk = cost => {
        const c = parseFloat(cost) || 0;
        if (c <= 0) return 0;
        return Math.ceil(((c + handling) * (1 + markupPct / 100) / (1 - 0.1335) + 0.30) * 100) / 100;
      };
      const variants = product.hasVariations
        ? buildVariants({ product, groupSku: normSku, applyMk, defaultQty, body })
        : [];
      const freshSkuSet = new Set(variants.map(v => v.sku));
      console.log(`[resync] fresh scrape produced ${variants.length} variants`);

      // STEP 5: classify zombies (on eBay but not in fresh scrape)
      const zombies = existingSkus.filter(s => !freshSkuSet.has(s));

      // STEP 6: enforce 50-cap by evicting oldest zombies first
      const totalAfterMerge = variants.length + zombies.length;
      let zombiesToEvict = [], zombiesToKeep = zombies;
      if (totalAfterMerge > HARD_CAP) {
        zombies.sort((a, b) => {
          const da = offerMap[a]?.lastModifiedDate || '';
          const db = offerMap[b]?.lastModifiedDate || '';
          return da.localeCompare(db) || existingSkus.indexOf(a) - existingSkus.indexOf(b);
        });
        const evictCount = totalAfterMerge - HARD_CAP;
        zombiesToEvict = zombies.slice(0, evictCount);
        zombiesToKeep  = zombies.slice(evictCount);
        console.log(`[resync] cap eviction: total=${totalAfterMerge} > ${HARD_CAP}, evicting ${evictCount} oldest zombies`);
      }

      // STEP 7: evict zombie offers (DELETE — eBay allows ending an offer)
      let evictedOk = 0, evictedFail = 0;
      for (const sku of zombiesToEvict) {
        const oid = offerMap[sku]?.offerId;
        if (!oid) continue;
        try {
          const dr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${encodeURIComponent(oid)}`, { method: 'DELETE', headers: auth });
          if (dr.ok || dr.status === 204) { evictedOk++; console.log(`[resync] ✗ evicted ${sku.slice(-20)}`); }
          else { evictedFail++; console.warn(`[resync] evict ${sku.slice(-20)} failed: ${dr.status}`); }
        } catch(e) { evictedFail++; }
      }

      // STEP 8: resolve policies + category for the fresh variant updates
      let policies = {};
      try { policies = await resolvePolicies(access_token, { fulfillmentPolicyId, paymentPolicyId, returnPolicyId }); }
      catch(e) { return res.status(400).json({ error: `Policy resolution failed: ${e.message}` }); }
      const locationKey = await ensureLocation(auth);
      const categoryId = product._categoryId || product.categoryId || grpData.aspects?.['eBay Category'] || '';

      // STEP 9: update fresh variants — PUT inventory_item + GET/PUT or POST offer
      let updOk = 0, updFail = 0;
      const groupAspects = product.aspects || {};
      for (let i = 0; i < variants.length; i += 5) {
        await Promise.all(variants.slice(i, i + 5).map(async v => {
          try {
            // eBay requires: aspect keys normalized to canonical names (Color, Size, etc.)
            // and aspect values wrapped in arrays of strings, not raw scalars.
            // Mismatch on either causes errorId 2004 "Invalid request".
            const itemAspects = {};
            for (const [k, av] of Object.entries(groupAspects || {})) {
              itemAspects[k] = Array.isArray(av) ? av : [String(av)];
            }
            for (const [k, val] of Object.entries(v.dims || {})) {
              const normKey = /color|colour/i.test(k) ? 'Color'
                            : /^size$/i.test(k)       ? 'Size'
                            : k;
              itemAspects[normKey] = [String(val)];
            }
            const _title = sanitizeTitle(product.ebayTitle || product.title || '');
            // Sanitize description: strip VERO brand names + banned words eBay rejects.
            // Without this, descriptions echoing brand names trigger 25019 "improper words"
            // and the entire publish fails.
            let _desc = product.description || product.title || _title || 'Product';
            _desc = stripVeROFromTitle(_desc); // strip VERO brand names from desc body
            const _BANNED_DESC = [/\bauthentic\b/gi, /\bgenuine\b/gi, /\boriginal\b/gi,
              /\bverified\b/gi, /\bcertified\b/gi, /\bauthorized\b/gi];
            for (const re of _BANNED_DESC) _desc = _desc.replace(re, '');
            _desc = _desc.replace(/\s{2,}/g, ' ').trim() || _title || 'Product';
            const ib = {
              condition: 'NEW',
              product: {
                title: _title,
                description: _desc, // REQUIRED — without this eBay returns 2004 Invalid request
                imageUrls: [v.image, ...(product.images || [])].filter(Boolean).slice(0, 12),
                aspects: itemAspects,
              },
              availability: { shipToLocationAvailability: { quantity: parseInt(v.qty) || 0 } },
            };
            const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(v.sku)}`, {
              method: 'PUT', headers: auth, body: JSON.stringify(ib),
            });
            if (!ir.ok && ir.status !== 204) {
              updFail++;
              console.warn(`[resync] inv-item PUT ${v.sku.slice(-20)} ${ir.status}: ${(await ir.text().catch(()=>'')).slice(0,500)}`);
              return;
            }
            // offer: update existing or create new
            const existing = offerMap[v.sku];
            if (existing?.offerId) {
              const gr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${encodeURIComponent(existing.offerId)}`, { headers: auth });
              if (!gr.ok) { updFail++; return; }
              const ob = await gr.json();
              ob.pricingSummary = { price: { value: String(v.price), currency: 'USD' } };
              ob.availableQuantity = parseInt(v.qty) || 0;
              delete ob.offerId; delete ob.status; delete ob.listing; delete ob.marketplaceId; delete ob.offerState;
              const pr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${encodeURIComponent(existing.offerId)}`, {
                method: 'PUT', headers: auth, body: JSON.stringify(ob),
              });
              if (pr.ok || pr.status === 204) { updOk++; console.log(`[resync] ✓ ${v.sku.slice(-20)} qty=${v.qty} $${v.price}`); }
              else { updFail++; console.warn(`[resync] offer PUT ${v.sku.slice(-20)} ${pr.status}`); }
            } else {
              const off = buildOffer(v.sku, String(v.price), categoryId, policies, locationKey);
              off.availableQuantity = parseInt(v.qty) || 0;
              const cr = await fetch(`${EBAY_API}/sell/inventory/v1/offer`, {
                method: 'POST', headers: auth, body: JSON.stringify(off),
              });
              if (cr.ok) { updOk++; console.log(`[resync] ✓ NEW ${v.sku.slice(-20)} qty=${v.qty} $${v.price}`); }
              else { updFail++; console.warn(`[resync] offer POST ${v.sku.slice(-20)} ${cr.status}`); }
            }
          } catch(e) { updFail++; console.warn(`[resync] update threw ${v.sku.slice(-20)}: ${e.message}`); }
        }));
        if (i + 5 < variants.length) await sleep(200);
      }

      // STEP 10: force-OOS surviving zombies
      let zKeptOk = 0;
      for (const sku of zombiesToKeep) {
        const oid = offerMap[sku]?.offerId;
        if (!oid) continue;
        try {
          const gr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${encodeURIComponent(oid)}`, { headers: auth });
          if (!gr.ok) continue;
          const ob = await gr.json();
          ob.availableQuantity = 0;
          const keepPrice = (offerMap[sku].currentPrice > 1.00) ? offerMap[sku].currentPrice : 9.99;
          ob.pricingSummary = { price: { value: keepPrice.toFixed(2), currency: 'USD' } };
          delete ob.offerId; delete ob.status; delete ob.listing; delete ob.marketplaceId; delete ob.offerState;
          const pr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${encodeURIComponent(oid)}`, {
            method: 'PUT', headers: auth, body: JSON.stringify(ob),
          });
          if (pr.ok || pr.status === 204) zKeptOk++;
        } catch(e) {}
      }

      // STEP 11: update the group's variantSKUs
      const finalSkus = [...freshSkuSet, ...zombiesToKeep];
      try {
        const gPut = { ...grpData, variantSKUs: finalSkus };
        delete gPut.inventoryItemGroupKey;
        await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(normSku)}`, {
          method: 'PUT', headers: auth, body: JSON.stringify(gPut),
        });
      } catch(e) { console.warn(`[resync] group PUT failed: ${e.message}`); }

      // STEP 12: publish
      let listingId = null;
      try {
        const pubR = await fetch(`${EBAY_API}/sell/inventory/v1/offer/publish_by_inventory_item_group`, {
          method: 'POST', headers: auth,
          body: JSON.stringify({ inventoryItemGroupKey: normSku, marketplaceId: 'EBAY_US' }),
        });
        const pd = await pubR.json().catch(() => ({}));
        if (pd.listingId) listingId = pd.listingId;
        else console.warn(`[resync] publish: ${JSON.stringify(pd.errors||{}).slice(0,800)}`);
      } catch(e) { console.warn(`[resync] publish threw: ${e.message}`); }

      console.log(`[resync] done ${normSku}: fresh=${updOk}/${variants.length}ok zombiesKept=${zKeptOk}/${zombiesToKeep.length} evicted=${evictedOk}/${zombiesToEvict.length} listingId=${listingId||'?'}`);
      return res.json({
        success: !!listingId || updOk > 0,
        listingId,
        freshOk: updOk, freshFail: updFail,
        zombiesEvicted: evictedOk, zombiesEvictFail: evictedFail,
        zombiesKept: zKeptOk, totalSkus: finalSkus.length,
      });
    }

    if (action === 'smartSync') {
      const { access_token, ebaySku, ebayListingId, sourceUrl,
              markup: mkRaw, handlingCost: handRaw, quantity: qtyRaw } = body;
      if (!access_token || !ebaySku || !sourceUrl)
        return res.status(400).json({ error: 'Missing access_token, ebaySku, or sourceUrl' });

      const EBAY_API   = getEbayUrls().EBAY_API;
      const markupPct  = parseFloat(mkRaw  ?? 23);
      const handling   = parseFloat(handRaw ?? 2);
      const defaultQty = parseInt(qtyRaw) || 1;
      const auth = {
        Authorization: `Bearer ${access_token}`,
        'Content-Type': 'application/json',
        'Content-Language': 'en-US',
        'Accept-Language': 'en-US',
      };
      const applyMk = cost => {
        const c = parseFloat(cost) || 0;
        if (c <= 0) return 0;
        return Math.ceil(((c + handling) * (1 + markupPct / 100) / (1 - 0.1335) + 0.30) * 100) / 100;
      };
      const normSku = ebaySku.trim().replace(/\s+/g, '');
      console.log(`[smartSync] ${normSku} — ${sourceUrl}`);

      // ── STEP 1: Scrape Amazon parent page ─────────────────────────────────
      // If the parent scrape fails (Amazon block, rate limit, network error), we
      // DO NOT bail out — the purpose of sync is to make eBay match Amazon's
      // dictated state, so we need to keep going even when the parent page is
      // unavailable. The frontend/worker already sends `comboAsin` and
      // `fallbackComboPrices` as a backup ASIN source derived from the last
      // successful scrape, so we use those to build the ASIN list and proceed
      // to per-ASIN fetching. Only genuinely-unreachable ASINs end up as
      // force-OOS via the fetchFailed path in STEP 2.
      const html = await fetchPage(sourceUrl, randUA()).catch(() => null);
      const _parentHtmlOk = !!(html && html.length >= 5000);

      // Extract unique ASINs from the parent page's dimensionToAsinMap.
      // Window widened to 200k chars to cover listings with 900+ ASIN entries.
      let dta = {};
      if (_parentHtmlOk) {
        const dtaM = html.match(/"dimensionToAsinMap"\s*:\s*(\{[^}]{0,200000}\})/);
        try { dta = JSON.parse(dtaM?.[1] || '{}'); } catch(e) {}
      }
      let allUniqueAsins = [...new Set(Object.values(dta))].filter(Boolean);

      // FALLBACK: if parent scrape failed OR the twister data was malformed,
      // use the stored comboAsin from the body (sent by frontend/worker as
      // the last known-good Amazon mapping). This lets sync still push fresh
      // per-ASIN prices even when Amazon is blocking the parent page.
      if (!allUniqueAsins.length && body.comboAsin && typeof body.comboAsin === 'object') {
        const _fallbackAsins = [...new Set(Object.values(body.comboAsin))].filter(Boolean);
        if (_fallbackAsins.length > 0) {
          allUniqueAsins = _fallbackAsins;
          console.log(`[smartSync] parent scrape ${_parentHtmlOk ? 'returned no dta' : 'BLOCKED'} — falling back to body.comboAsin (${allUniqueAsins.length} ASINs)`);
        }
      }
      const _urlAsin = sourceUrl.match(/\/dp\/([A-Z0-9]{10})/)?.[1];
      if (!allUniqueAsins.length && _urlAsin) allUniqueAsins.push(_urlAsin);

      // Only bail if we have absolutely nothing to sync — no fresh parent data
      // AND no stored comboAsin AND no URL ASIN. In that case there's literally
      // nothing we can fetch.
      if (!allUniqueAsins.length) {
        return res.json({
          success: false,
          error: 'Amazon parent scrape blocked and no fallback comboAsin available — cannot determine ASIN list',
          skippable: true,
          parentHtmlOk: _parentHtmlOk,
        });
      }

      // ── Progressive slicing — 50 ASINs per cycle ─────────────────────────────
      // Capped at 50 per user rule (was 100). Smaller batches = lower per-cycle
      // load on Amazon (less rate-limit triggering) and faster cycle turnaround,
      // at the cost of more cycles for very large listings (1000+ variations).
      // Frontend stores asinOffset, sends it each cycle, resets when all done.
      const ASIN_BATCH_SIZE = 50;
      const asinOffset = parseInt(body.asinOffset || 0);
      const _slice = allUniqueAsins.slice(asinOffset, asinOffset + ASIN_BATCH_SIZE);
      const uniqueAsins = _slice.length ? _slice : allUniqueAsins; // fallback to all if offset wrong
      const nextAsinOffset = asinOffset + uniqueAsins.length >= allUniqueAsins.length ? 0 : asinOffset + ASIN_BATCH_SIZE;

      console.log(`[smartSync] ${allUniqueAsins.length} ASINs total — fetching ${uniqueAsins.length} (offset ${asinOffset})${nextAsinOffset ? ` → next: ${nextAsinOffset}` : ' → complete'}`);

      // ── STEP 2: Fetch each ASIN page → price (fail fast, 2 attempts max) ───────
      // Failed fetches used to leave asinPrice[asin] undefined which caused the
      // variant to silently hit _skippedNoBatch in the main update loop, preserving
      // whatever stale price/qty was last on eBay. That broke the user's ability
      // to undo manual price/qty edits by running a sync: if one of the failed
      // ASINs was a variant they'd touched, the sync would appear to succeed but
      // those specific variants wouldn't actually be updated. Fix: always set
      // asinPrice[asin] = 0 on fetch failure and mark inStock=false. Downstream,
      // cost=0 naturally forces qty=0 and preserves the existing eBay offer price.
      // This means a transient Amazon block briefly OOSes a variant (safer than
      // selling at an unknown wrong price), and the next sync restores it.
      const asinPrice   = {};
      const asinInStock = {};
      const _fetchFailed = new Set(); // ASINs where fetchPage returned null
      if (uniqueAsins.length > 0) {
        console.log(`[smartSync] ── Batch ${Math.floor(asinOffset/ASIN_BATCH_SIZE)+1}: ASINs ${asinOffset+1}–${asinOffset+uniqueAsins.length} of ${allUniqueAsins.length} ──`);
        const BATCH = 6;
        let fetchOk = 0, fetchFail = 0;
        for (let i = 0; i < uniqueAsins.length; i += BATCH) {
          await Promise.all(uniqueAsins.slice(i, i + BATCH).map(async (asin, bi) => {
            await sleep(bi * 150);
            // 2 attempts max — blocked ASINs are set qty=0 this cycle (Out-of-Stock
            // Control hides the variant) and re-tried normally on the next sync.
            // This is NOT a real OOS event: no markedOos flag, no OOS webhook.
            const h = await fetchPage(`https://www.amazon.com/dp/${asin}?th=1&psc=1`, randUA(), 2);
            if (!h) {
              asinPrice[asin]   = 0;     // ← explicit 0 so _haveFreshPrice sees it
              asinInStock[asin] = false;
              _fetchFailed.add(asin);
              fetchFail++;
              console.log(`[smartSync] ${asin} → fetch blocked, qty=0 (will retry next sync)`);
              return;
            }
            const price = extractPriceFromBuyBox(h) || extractPrice(h) || 0;
            const oos   = isAmazonOOS(h.match(/id="availability"[\s\S]{0,3000}/)?.[0] || '');
            asinPrice[asin]   = price;
            asinInStock[asin] = !oos && price > 0;
            console.log(`[smartSync] ${asin} → $${price} inStock=${!oos}`);
            fetchOk++;
          }));
          if (i + BATCH < uniqueAsins.length) await sleep(1000);
        }
        console.log(`[smartSync] ── Batch ${Math.floor(asinOffset/ASIN_BATCH_SIZE)+1} done: ${fetchOk} ok, ${fetchFail} blocked (qty=0 this cycle) ──`);
      }

      // (removed _bestCost / _bestEbay sibling-price fallback — per user rule
      // each variant must use its OWN ASIN's fresh Amazon price or go qty=0,
      // never inherit a price from a different variant. Keeping these defined
      // would risk future code accidentally wiring them in as a fallback.)

      // ── STEP 3: Get variant SKUs + offer IDs ─────────────────────────────────
      const skuToAsin      = body.skuToAsin     || {};
      const cachedOfferIds = body.cachedOfferIds || {};
      const offerMap       = {}; // sku → { offerId, currentPrice }
      let _listingId       = String(body.ebayListingId || '');
      const allVariantSkus = new Set(Object.keys(skuToAsin));

      // Fast path: cached offer IDs from worker/frontend — skip full discovery chain
      if (Object.keys(cachedOfferIds).length > 0) {
        for (const [sku, offerId] of Object.entries(cachedOfferIds)) {
          if (sku && offerId) offerMap[sku] = { offerId, currentPrice: 0 };
        }
        console.log(`[smartSync] using ${Object.keys(offerMap).length} cached offer IDs — skipping discovery`);
      }

      // Step 3a: GET inventory_item_group → variantSKUs (skipped if cached IDs found)
      if (Object.keys(offerMap).length === 0) {
      const grpR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(normSku)}`, { headers: auth }).catch(() => null);
      const grpData = grpR?.ok ? await grpR.json().catch(() => ({})) : {};
      for (const sku of (grpData.variantSKUs || [])) allVariantSkus.add(sku);
      if (!_listingId && grpData.listing?.listingId) _listingId = String(grpData.listing.listingId);
      console.log(`[smartSync] group has ${grpData.variantSKUs?.length || 0} SKUs, skuToAsin has ${Object.keys(skuToAsin).length}`);

      // Step 3b: if still few SKUs, scan offers by listingId to find more
      if (allVariantSkus.size < 3 && _listingId) {
        let _off = 0;
        while (_off < 500) {
          const _or = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=100&offset=${_off}`, { headers: auth }).catch(() => null);
          if (!_or?.ok) break;
          const _od = await _or.json().catch(() => ({}));
          for (const o of (_od.offers || []))
            if (o.sku && o.listing?.listingId === _listingId) allVariantSkus.add(o.sku);
          if ((_od.offers||[]).length < 100) break;
          _off += 100; await sleep(80);
        }
      }

      // Step 3c: GET offer for every variant SKU — batches of 20 in parallel
      const skuList = [...allVariantSkus];
      for (let i = 0; i < skuList.length; i += 20) {
        await Promise.all(skuList.slice(i, i + 20).map(async sku => {
          const r = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}&limit=1`, { headers: auth }).catch(() => null);
          if (r?.ok) {
            const d = await r.json().catch(() => ({}));
            const o = (d.offers || [])[0];
            if (o?.offerId) offerMap[sku] = { offerId: o.offerId, currentPrice: parseFloat(o.pricingSummary?.price?.value || 0) };
          }
        }));
        if (i + 20 < skuList.length) await sleep(60);
      }

      // Step 3d: if still 0 offers, try method 4 (predict SKUs from dta + hash)
      if (Object.keys(offerMap).length === 0 && Object.keys(dta).length > 0 && html) {
        const vvM3 = html.match(/"variationValues"\s*:\s*(\{(?:[^{}]|\{[^{}]*\})*\})/);
        let vv3 = {};
        try { vv3 = JSON.parse(vvM3?.[1] || '{}'); } catch(e) {}
        const _slug3 = s => (s||'').replace(/[^A-Z0-9]/gi,'_').toUpperCase().replace(/_+/g,'_').replace(/^_|_$/g,'');
        const _dimKeys3 = Object.keys(vv3);
        const _pfx3 = normSku + '-'; const maxSfx3 = 50 - _pfx3.length;
        const _mkSku3 = parts => { const raw = parts.map(_slug3).filter(Boolean).join('_'); if (raw.length <= maxSfx3) return _pfx3+raw; const hash = raw.split('').reduce((h,c)=>((h<<5)-h+c.charCodeAt(0))|0,0); return _pfx3+raw.slice(0,maxSfx3-9)+'_'+(hash>>>0).toString(16).toUpperCase().padStart(8,'0'); };
        const predicted = new Set();
        for (const [idx] of Object.entries(dta)) { const p3=String(idx).split('_'); const v3=_dimKeys3.map((k,ki)=>(vv3[k]||[])[parseInt(p3[ki]??p3[0])||0]||'').filter(Boolean); if(v3.length) predicted.add(_mkSku3(v3)); }
        console.log(`[smartSync] method 4: trying ${predicted.size} predicted SKUs`);
        for (let i=0;i<[...predicted].length;i+=20) {
          await Promise.all([...predicted].slice(i,i+20).map(async sku => {
            const r=await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}&limit=1`,{headers:auth}).catch(()=>null);
            if(r?.ok){const d=await r.json().catch(()=>({}));const o=(d.offers||[])[0];if(o?.offerId)offerMap[sku]={offerId:o.offerId,currentPrice:parseFloat(o.pricingSummary?.price?.value||0)};}
          }));
          if(i+20<[...predicted].length) await sleep(60);
        }
      }

            } // end discovery chain

      console.log(`[smartSync] found ${Object.keys(offerMap).length} offer IDs total`);
      if (Object.keys(offerMap).length === 0)
        return res.json({ success: false, error: 'No offers found — listing may need re-push' });

      // ── STEP 4: Match each offer SKU to an ASIN → get price ──────────────────
      // Method A: skuToAsin (sent from frontend, built at push time) — most reliable
      // Method B: variationValues + dta → displayValue→ASIN map
      // Method C: inventory item aspects → displayValue lookup

      // Guard html.match — parent scrape may have failed (we're now in fallback mode)
      let vv2 = {};
      if (html) {
        const vvM2 = html.match(/"variationValues"\s*:\s*(\{(?:[^{}]|\{[^{}]*\})*\})/);
        try { vv2 = JSON.parse(vvM2?.[1] || '{}'); } catch(e) {}
      }

      // CRITICAL: Read the authoritative dimension order from Amazon's "dimensions"
      // array. Object.keys(vv2) cannot be trusted — on many listings (e.g. LIANLAM
      // rugs B0G3NJ2L3Z) `variationValues` is serialized as {size_name,color_name}
      // while `dimensions` says ["color_name","size_name"]. The `dimensionToAsinMap`
      // keys ("0_1", "1_0", ...) are ALWAYS ordered per the `dimensions` array, so
      // using the wrong key order silently swaps color and size on every variant
      // and produces completely wrong SKU→ASIN mappings.
      let _dimOrder = [];
      if (html) {
        const _dimOrderM = html.match(/"dimensions"\s*:\s*(\[[^\]]{0,400}\])/);
        try { _dimOrder = JSON.parse(_dimOrderM?.[1] || '[]'); } catch(e) {}
      }
      // Fallback: use fallbackVariations from body to infer dim names when parent is blocked
      if (!_dimOrder.length && Array.isArray(body.fallbackVariations) && body.fallbackVariations.length > 0) {
        _dimOrder = body.fallbackVariations.map(v => String(v.name||'').toLowerCase().replace(/\s+/g, '_')).filter(Boolean);
      }
      const _authDimKeys = (_dimOrder.length ? _dimOrder : Object.keys(vv2))
        .filter(k => Array.isArray(vv2[k]) && vv2[k].length > 0);

      // Build valToAsin directly from dimensionToAsinMap entries, using the correct
      // dim key order. For each "idx0_idx1" entry, look up vv2[dimKey][idx] for each
      // dimension position. This replaces the old code which did `dta[String(idx)]`
      // where `idx` was a single digit — that only worked for single-dim products.
      const valToAsin = {};
      for (const [dtaKey, asin] of Object.entries(dta || {})) {
        if (!asin) continue;
        const parts = String(dtaKey).split('_');
        for (let pi = 0; pi < _authDimKeys.length && pi < parts.length; pi++) {
          const dimKey = _authDimKeys[pi];
          const val    = (vv2[dimKey] || [])[parseInt(parts[pi]) || 0];
          if (val && !valToAsin[String(val).toLowerCase()]) {
            valToAsin[String(val).toLowerCase()] = asin;
          }
        }
      }
      // Fallback: if dta was empty (parent blocked), also populate valToAsin from
      // body.comboAsin keys. comboAsin keys are already in "PrimVal|SecVal" form,
      // so we can slice out each dim value and map to the ASIN.
      if (Object.keys(valToAsin).length === 0 && body.comboAsin && typeof body.comboAsin === 'object') {
        for (const [comboKey, asin] of Object.entries(body.comboAsin)) {
          if (!asin) continue;
          const parts = String(comboKey).split(/\s*\|\s*|\s*\/\s*/).map(s => s.trim()).filter(Boolean);
          for (const p of parts) {
            const k = p.toLowerCase();
            if (k && !valToAsin[k]) valToAsin[k] = asin;
          }
        }
      }
      console.log(`[smartSync] valToAsin: ${Object.keys(valToAsin).length} entries, skuToAsin: ${Object.keys(skuToAsin).length} entries, dimOrder: [${_authDimKeys.join(',')}]${!html ? ' (parent BLOCKED, using fallback)' : ''}`);

      // Reconstruct skuToAsin from dta + variationValues + offer SKU suffixes for any SKUs
      // not already covered by Method A. This is needed for old listings whose variant SKUs
      // were truncated/hashed by an earlier push code (different SKU_MAX) — the worker
      // pre-builds skuToAsin in the current 50-char format, so direct lookup misses every
      // truncated variant. Reconstruction matches dim-value slugs against the variant SKU
      // text, which works regardless of how the SKU was truncated.
      const _needsRecon = new Set();
      for (const sku of Object.keys(offerMap)) {
        if (!skuToAsin[sku]) _needsRecon.add(sku);
      }
      // Reconstruction path 1: use parent's dta + vv2 (most accurate when available)
      // Reconstruction path 2: use body.comboAsin directly (fallback when parent blocked)
      const _canReconFromDta = Object.keys(dta).length > 0 && _authDimKeys.length > 0;
      const _canReconFromCombo = body.comboAsin && typeof body.comboAsin === 'object' && Object.keys(body.comboAsin).length > 0;
      if (_needsRecon.size > 0 && (_canReconFromDta || _canReconFromCombo)) {
        const _slug = s => (s||'').replace(/[^A-Z0-9]/gi,'_').toUpperCase().replace(/_+/g,'_').replace(/^_|_$/g,'');
        // Use the authoritative dim key order parsed from Amazon's "dimensions" array,
        // NOT Object.keys(vv2). See the big comment above — getting this wrong silently
        // swaps color/size on every variant.
        const _dimKeys = _authDimKeys;
        // Build slug → ASIN from dta. For each dimensionToAsinMap entry, take the
        // combination of dim values it points to and slugify them.
        const slugToAsin = {};
        // Also build reversed-order slugs — eBay SKUs may store dims in a different
        // order than Amazon (e.g. Amazon "color_name, size_name" vs eBay SKU
        // "SIZE_COLOR" or "COLOR_SIZE"). We accept both directions.
        const slugToAsinReversed = {};
        // Path 1: parent dta iteration
        if (_canReconFromDta) {
        for (const [idx, asin] of Object.entries(dta)) {
          const parts = String(idx).split('_');
          const vals = _dimKeys.map((k, ki) => {
            const arr = vv2[k] || [];
            return arr[parseInt(parts[ki] ?? parts[0]) || 0] || '';
          }).filter(Boolean);
          if (vals.length) {
            // Full slug (all dims joined in Amazon's order) — most specific
            const slug = vals.map(_slug).join('_');
            if (slug) slugToAsin[slug] = asin;
            // Reversed-order slug — for eBay SKUs that flipped dim order at push time
            if (vals.length > 1) {
              const revSlug = vals.slice().reverse().map(_slug).join('_');
              if (revSlug && !slugToAsinReversed[revSlug]) slugToAsinReversed[revSlug] = asin;
            }
            // Primary-dim-only slug — fallback for SKUs that hashed/truncated the secondary dim
            if (vals.length > 1) {
              const primary = _slug(vals[0]);
              if (primary && !slugToAsin[primary]) slugToAsin[primary] = asin;
              // Also secondary-only, but only as a last-resort (lowest priority)
              const secondary = _slug(vals[1]);
              if (secondary && !slugToAsin[secondary] && !slugToAsinReversed[secondary]) {
                slugToAsinReversed[secondary] = asin;
              }
            }
          }
        }
        } // end path 1 (_canReconFromDta)

        // Path 2: body.comboAsin fallback — runs in addition to path 1 so stored
        // combo entries fill any gaps the parent-scrape-based path couldn't cover
        // (and is the ONLY source when parent is blocked).
        if (_canReconFromCombo) {
          for (const [comboKey, asin] of Object.entries(body.comboAsin)) {
            if (!asin) continue;
            // comboKey format from scraper: "PrimVal|SecVal" or "PrimVal|SecVal / Size"
            const rawParts = String(comboKey).split(/\s*\|\s*|\s*\/\s*/).map(s => s.trim()).filter(Boolean);
            if (rawParts.length === 0) continue;
            // Full compound slug
            const fullSlug = rawParts.map(_slug).join('_');
            if (fullSlug && !slugToAsin[fullSlug]) slugToAsin[fullSlug] = asin;
            // Reversed slug
            if (rawParts.length > 1) {
              const revSlug = rawParts.slice().reverse().map(_slug).join('_');
              if (revSlug && !slugToAsinReversed[revSlug]) slugToAsinReversed[revSlug] = asin;
            }
            // Primary-dim-only (last resort)
            if (rawParts.length > 1) {
              const primary = _slug(rawParts[0]);
              if (primary && !slugToAsin[primary]) slugToAsin[primary] = asin;
            }
          }
        }

        // CRITICAL: sort slugs by length DESCENDING so the most specific slug matches first.
        // Without this, "BUTTERFLY" can match a variant whose actual color is "LAVENDER BUTTERFLY"
        // because both .includes() the substring — whichever is iterated first wins.
        // Primary (Amazon-order) slugs take precedence over reversed-order slugs.
        const _sortedSlugs = [
          ...Object.entries(slugToAsin),
          ...Object.entries(slugToAsinReversed),
        ].sort((a, b) => b[0].length - a[0].length);
        const _pfxUpper = normSku.toUpperCase() + '-';
        let _reconHits = 0;
        for (const sku of _needsRecon) {
          // Strip the group SKU prefix when present, then uppercase for case-insensitive matching
          const sfx = (sku.toUpperCase().startsWith(_pfxUpper)
            ? sku.slice(normSku.length + 1)
            : sku).toUpperCase();
          for (const [slug, asin] of _sortedSlugs) {
            if (sfx.startsWith(slug) || sfx.includes(slug)) {
              skuToAsin[sku] = asin;
              _reconHits++;
              break;
            }
          }
        }
        console.log(`[smartSync] reconstruction matched ${_reconHits}/${_needsRecon.size} uncovered SKUs (${Object.keys(offerMap).length} total, dimOrder=[${_dimKeys.join(',')}]${_canReconFromCombo?' +comboAsin fallback':''})`);
        // Stash the sorted slug map for aspect-based reconstruction below
        global.__smartSyncSlugMap = _sortedSlugs;
        global.__smartSyncSlug    = _slug;
      }

      // GET inventory item aspects — ALWAYS, not just when valToAsin is empty. We
      // use aspects to disambiguate truncated/hashed SKUs that the slug reconstruction
      // could only match via primary-only fallback (e.g. "CHOCOLATE_10_X_14__9EBE3186"
      // only matches the "CHOCOLATE" primary slug and grabs the first Chocolate ASIN,
      // which is invariably the 2'x3' variant — cheapest, smallest). With aspects
      // available we build a compound slug from aspect values and look up the exact
      // variant.
      const skuAspects = {};
      {
        const offerSkus = Object.keys(offerMap);
        for (let i = 0; i < offerSkus.length; i += 25) {
          const batch = offerSkus.slice(i, i + 25);
          try {
            const bgr = await fetch(`${EBAY_API}/sell/inventory/v1/bulk_get_inventory_item`, {
              method: 'POST', headers: auth,
              body: JSON.stringify({ requests: batch.map(s => ({ sku: s })) }),
            });
            const bgd = await bgr.json().catch(() => ({}));
            for (const resp of (bgd.responses || []))
              if (resp.sku && resp.inventoryItem?.product?.aspects)
                skuAspects[resp.sku] = resp.inventoryItem.product.aspects;
          } catch(e) {}
          if (i + 25 < offerSkus.length) await sleep(100);
        }
      }

      // Aspect-based compound-slug upgrade: for every SKU (including ones already
      // matched above via primary-only fallback), try to build a compound slug from
      // its aspects and look it up. If it resolves to a different ASIN than the
      // primary-only match, prefer the aspect match — it's strictly more specific.
      if (Object.keys(skuAspects).length > 0 && global.__smartSyncSlugMap) {
        const _slug = global.__smartSyncSlug;
        const _slugMap = Object.fromEntries(global.__smartSyncSlugMap);
        let _aspectUpgrades = 0;
        let _aspectNewMatches = 0;
        for (const sku of Object.keys(offerMap)) {
          const aspects = skuAspects[sku];
          if (!aspects || Object.keys(aspects).length === 0) continue;
          // Flatten all aspect values (e.g. Color=['Chocolate'], Size=['10\' x 14\' (Rectangular)'])
          const aspectVals = [];
          for (const vals of Object.values(aspects)) {
            for (const v of (Array.isArray(vals) ? vals : [vals])) {
              if (v && typeof v === 'string') aspectVals.push(v);
            }
          }
          if (aspectVals.length === 0) continue;
          // Try every permutation of 2 values (covers color-first + size-first orderings)
          // For compound keys with 2+ parts, we generate both orderings.
          let _bestAsin = null;
          if (aspectVals.length >= 2) {
            // Build both orderings of the first two aspect values
            const slugs = [
              aspectVals.map(_slug).join('_'),                       // all aspects in object order
              aspectVals.slice().reverse().map(_slug).join('_'),     // reversed
              [aspectVals[0], aspectVals[1]].map(_slug).join('_'),   // first two only
              [aspectVals[1], aspectVals[0]].map(_slug).join('_'),   // first two reversed
            ];
            for (const s of slugs) {
              if (_slugMap[s]) { _bestAsin = _slugMap[s]; break; }
            }
          }
          // Fallback: single aspect value direct lookup
          if (!_bestAsin) {
            for (const v of aspectVals) {
              const s = _slug(v);
              if (_slugMap[s]) { _bestAsin = _slugMap[s]; break; }
            }
          }
          if (_bestAsin) {
            if (!skuToAsin[sku]) {
              skuToAsin[sku] = _bestAsin;
              _aspectNewMatches++;
            } else if (skuToAsin[sku] !== _bestAsin) {
              skuToAsin[sku] = _bestAsin;
              _aspectUpgrades++;
            }
          }
        }
        console.log(`[smartSync] aspect-slug upgrade: ${_aspectUpgrades} corrected, ${_aspectNewMatches} new matches (from ${Object.keys(skuAspects).length} SKUs with aspects)`);
        delete global.__smartSyncSlugMap;
        delete global.__smartSyncSlug;
      }

      // Pre-sort valToAsin entries by key length desc — same reason as the reconstruction
      // path: longer values must be checked first so "lavender butterfly" wins over "butterfly"
      // when both are .includes() in the SKU.
      const _sortedValToAsin = Object.entries(valToAsin).sort((a, b) => b[0].length - a[0].length);

      // Pre-compute the set of ASINs we have fresh data for. Variants whose ASIN isn't
      // in this set are OUTSIDE the current fetch batch (asinOffset slice) and must be
      // preserved as-is — touching them with cost=0 would OOS every variant beyond the
      // first 100 on every worker cycle. The worker never advances asinOffset, so without
      // this guard a 900-ASIN listing would flip 800 variants to qty=0 forever.
      const _haveFreshPrice = new Set(uniqueAsins.filter(a => asinPrice[a] !== undefined));

      const updates = [];
      const _skippedNoBatch = [];
      const _orphanedSkus = [];
      for (const [sku, offer] of Object.entries(offerMap)) {
        let asin = null;

        // Method A: direct skuToAsin lookup (most reliable)
        if (skuToAsin[sku]) {
          asin = skuToAsin[sku];
        }

        // Method B: valToAsin via aspects
        if (!asin && _sortedValToAsin.length > 0) {
          const aspects = skuAspects[sku] || {};
          for (const vals of Object.values(aspects)) {
            for (const v of (Array.isArray(vals) ? vals : [vals])) {
              const a = valToAsin[String(v).toLowerCase()];
              if (a) { asin = a; break; }
            }
            if (asin) break;
          }
          // Also try matching valToAsin against the SKU suffix directly — sorted by length
          // descending so the most specific dimension value matches first.
          if (!asin) {
            const _skuLower = sku.toLowerCase();
            for (const [val, a] of _sortedValToAsin) {
              if (_skuLower.includes(val.replace(/\s+/g, '_'))) { asin = a; break; }
            }
          }
        }

        // Last resort: single ASIN product
        if (!asin && uniqueAsins.length === 1) asin = uniqueAsins[0];

        // If still no ASIN match — this is an ORPHAN variant: it exists on eBay but
        // no longer corresponds to any ASIN on the current Amazon parent page. This
        // commonly happens when Amazon consolidates/removes colors or sizes. The old
        // behavior was to `continue` and preserve whatever stale price was there,
        // which meant orphan variants would keep selling at wrong prices indefinitely
        // (e.g. a 2'x6' runner priced at $278.71 because historical sync assigned
        // the 10'x14' cost by accident). Force qty=0 so the variant is un-sellable,
        // keep the existing price (eBay requires > 0). The user can re-push to drop
        // orphans entirely, or the variant will simply stop being offered.
        if (!asin) {
          _orphanedSkus.push(sku);
          // eBay rejects prices at or below their per-category minimum (often $1.00 strict).
          // Use the existing offer price if it's safely above the minimum, otherwise
          // use a $9.99 placeholder. qty=0 below ensures the placeholder is unbuyable.
          const preservePrice = offer.currentPrice > 1.00 ? offer.currentPrice : 9.99;
          console.log(`[smartSync] ${sku.slice(-24)} → ORPHAN (no ASIN match on current Amazon parent), forcing qty=0 price=$${preservePrice}`);
          updates.push({
            offerId:           offer.offerId,
            sku,
            availableQuantity: 0,
            price:             { value: preservePrice.toFixed(2), currency: 'USD' },
            _orphan:           true,
          });
          continue;
        }

        // ── ACCURACY GUARD ─────────────────────────────────────────────────────
        // If the matched ASIN is outside the current fetch batch, skip entirely.
        // Preserves the variant's existing qty + price rather than OOSing it.
        if (!_haveFreshPrice.has(asin)) {
          _skippedNoBatch.push(sku);
          continue;
        }

        const cost      = asinPrice[asin] || 0;
        const inStock   = asinInStock[asin] !== false;
        const ebayPrice = cost > 0 ? applyMk(cost) : 0;
        let qty         = (inStock && ebayPrice > 0) ? defaultQty : 0;
        // eBay rejects prices at or below their per-category minimum (often $1.00 strict).
        // Three cases for putPrice:
        //   1. Real ebayPrice from a real Amazon cost → use it
        //   2. Existing offer price > $1.00 → preserve it (still > min)
        //   3. Otherwise → $9.99 placeholder
        // ENFORCE qty=0 whenever we use the placeholder so no buyer can transact
        // at a fake price. This is the user's non-negotiable rule applied here.
        let putPrice;
        if (ebayPrice > 0) {
          putPrice = ebayPrice;
        } else if (offer.currentPrice > 1.00) {
          putPrice = offer.currentPrice;
          qty = 0; // no real Amazon price → unbuyable, regardless of what inStock said
        } else {
          putPrice = 9.99;
          qty = 0; // placeholder → unbuyable, non-negotiable
        }

        console.log(`[smartSync] ${sku.slice(-20)} asin=${asin||'?'} $${cost} → eBay $${putPrice.toFixed(2)} qty=${qty}`);
        updates.push({
          offerId:           offer.offerId,
          sku,
          availableQuantity: qty,
          price:             { value: putPrice.toFixed(2), currency: 'USD' },
        });
      }
      if (_skippedNoBatch.length > 0) {
        console.log(`[smartSync] preserved ${_skippedNoBatch.length} variants whose ASIN is outside this batch (will be touched on a later cycle when asinOffset advances)`);
      }
      if (_orphanedSkus.length > 0) {
        console.log(`[smartSync] forced OOS on ${_orphanedSkus.length} orphan variants: ${_orphanedSkus.slice(0,5).map(s => s.slice(-24)).join(', ')}${_orphanedSkus.length > 5 ? '…' : ''}`);
      }

      // ── STEP 5: Update all variants ──────────────────────────────────────────
      console.log(`[smartSync] updating ${updates.length} variants…`);
      let okCount = 0, failCount = 0;
      // Track failure reasons so we know whether failures are rate limits, stale
      // offers, auth issues, or malformed bodies. Without this, failures are
      // invisible and we can't tell why eBay didn't apply qty updates.
      const _failReasons = { get429: 0, get4xx: 0, get5xx: 0, getNet: 0,
                             put429: 0, put4xx: 0, put5xx: 0, putNet: 0 };

      // Helper: fetch with one retry on 429 (eBay rate-limit), backing off ~1.5s.
      // Most "silent" failures we've been seeing are 429s during the 10-parallel
      // burst. A single retry catches the vast majority.
      async function _fetchRetry(url, opts) {
        let r;
        try { r = await fetch(url, opts); } catch(e) { return { ok: false, status: 0, _net: e.message }; }
        if (r.status === 429) {
          await sleep(1500);
          try { r = await fetch(url, opts); } catch(e) { return { ok: false, status: 0, _net: e.message, _retried: true }; }
          r._retried = true;
        }
        return r;
      }

      // PUT /offer/{id} per variant — updates price + qty directly on the offer
      // No inventory item bulk update — it overwrites product data and breaks group association (25604)
      // Must use individual PUT (not bulk_update) — bulk fails on ENDED offers
      for (let i = 0; i < updates.length; i += 10) {
        await Promise.all(updates.slice(i, i + 10).map(async ({ offerId, sku, availableQuantity, price }) => {
          try {
            const gr = await _fetchRetry(`${EBAY_API}/sell/inventory/v1/offer/${encodeURIComponent(offerId)}`, { headers: auth });
            if (!gr.ok) {
              failCount++;
              if (gr._net)              _failReasons.getNet++;
              else if (gr.status === 429) _failReasons.get429++;
              else if (gr.status >= 500)  _failReasons.get5xx++;
              else                        _failReasons.get4xx++;
              const _retag = gr._retried ? ' (after 429-retry)' : '';
              console.warn(`[smartSync] GET offer ${(sku||offerId).slice(-18)} ${gr.status || 'NETERR'}${_retag}: ${(gr._net||'').slice(0,80)}`);
              return;
            }
            const offerBody = await gr.json();

            // Diff check DISABLED — force PUT every variant every sync.
            // (The check was causing some variants with stale offer-side data to
            // never get updated. Push always.)

            offerBody.pricingSummary = { price };
            offerBody.availableQuantity = availableQuantity;
            delete offerBody.offerId; delete offerBody.status; delete offerBody.listing;
            delete offerBody.marketplaceId; delete offerBody.offerState;
            const pr = await _fetchRetry(`${EBAY_API}/sell/inventory/v1/offer/${encodeURIComponent(offerId)}`, {
              method: 'PUT', headers: auth, body: JSON.stringify(offerBody),
            });
            if (pr.ok || pr.status === 204) {
              okCount++;
              console.log(`[smartSync] ✓ ${(sku||offerId).slice(-18)} qty=${availableQuantity} $${price.value}`);
            } else {
              failCount++;
              if (pr._net)               _failReasons.putNet++;
              else if (pr.status === 429) _failReasons.put429++;
              else if (pr.status >= 500)  _failReasons.put5xx++;
              else                        _failReasons.put4xx++;
              const e = pr._net || await pr.text().catch(() => '');
              const _retag = pr._retried ? ' (after 429-retry)' : '';
              console.warn(`[smartSync] PUT offer ${(sku||offerId).slice(-18)} ${pr.status || 'NETERR'}${_retag}: ${e.slice(0,500)}`);
            }
          } catch(e) {
            failCount++;
            _failReasons.putNet++;
            console.warn(`[smartSync] update threw for ${(sku||offerId).slice(-18)}: ${e.message}`);
          }
        }));
        if (i + 10 < updates.length) await sleep(250); // was 150ms — bumped to ease eBay rate limit
      }

      // Log failure breakdown so we can see what's actually going wrong
      if (failCount > 0) {
        const _bd = Object.entries(_failReasons).filter(([,n]) => n > 0)
                          .map(([k,n]) => `${k}=${n}`).join(' ');
        console.warn(`[smartSync] failure breakdown: ${_bd}`);
      }
      // Sync inventory item quantity to match offer — prevents OOS showing on eBay
      // even when offer qty=1, because eBay uses inventory item qty as the source of truth
      if (okCount > 0) {
        const _itemUpdates = updates.filter(u => u.availableQuantity > 0); // only need to fix in-stock
        for (let _qi = 0; _qi < _itemUpdates.length; _qi += 5) {
          await Promise.all(_itemUpdates.slice(_qi, _qi + 5).map(async ({ sku, availableQuantity }) => {
            try {
              const _ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, { headers: auth });
              if (!_ir.ok) return;
              const _id = await _ir.json().catch(() => ({}));
              if (!_id.condition) return;
              const _ib = {
                condition: _id.condition,
                product: _id.product || {},
                availability: { shipToLocationAvailability: { quantity: availableQuantity } },
              };
              if (_id.conditionId) _ib.conditionId = _id.conditionId;
              // Only include packageWeightAndSize if weight.value is a valid positive number.
              // Some listings have weight.value=0 or missing — eBay returns 25709 if we echo it back.
              if (_id.packageWeightAndSize) {
                const _wv = parseFloat(_id.packageWeightAndSize?.weight?.value || 0);
                if (_wv > 0) _ib.packageWeightAndSize = _id.packageWeightAndSize;
              }
              const _pr = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
                method: 'PUT', headers: auth, body: JSON.stringify(_ib),
              });
              if (!_pr.ok && _pr.status !== 204) {
                const _et = await _pr.text().catch(() => '');
                console.warn(`[smartSync] inv-item PUT ${sku.slice(-20)} ${_pr.status}: ${_et.slice(0,400)}`);
              }
            } catch(_qe) {
              console.warn(`[smartSync] inv-item PUT ${sku.slice(-20)} threw: ${_qe.message}`);
            }
          }));
          if (_qi + 5 < _itemUpdates.length) await sleep(100);
        }
        if (_itemUpdates.length > 0)
          console.log(`[smartSync] synced inventory item qtys: ${_itemUpdates.length} SKUs`);
      }

      console.log(`[smartSync] done — ${okCount} ok, ${failCount} failed`);

      // If ALL offers failed and we used cached IDs, the cache is stale (re-push changed offer IDs)
      // Signal worker to clear stale cache so next cycle does fresh discovery
      if (okCount === 0 && failCount > 0 && Object.keys(cachedOfferIds).length > 0) {
        console.warn(`[smartSync] all ${failCount} GETs/PUTs failed with cached offer IDs — cache is stale`);
        return res.json({
          success: false,
          error: 'Offer IDs stale — cache cleared, fresh discovery on next cycle',
          offersCacheStale: true,
          synced: 0, failed: failCount,
          nextAsinOffset, totalAsins: allUniqueAsins.length,
        });
      }

      // Publish — offer PUTs are already live even if publish fails
      try {
        const pubR = await fetch(`${EBAY_API}/sell/inventory/v1/offer/publish_by_inventory_item_group`,
          { method: 'POST', headers: auth,
            body: JSON.stringify({ inventoryItemGroupKey: normSku, marketplaceId: 'EBAY_US' }) });
        const pubTxt = await pubR.text();
        let pubD = {}; try { pubD = JSON.parse(pubTxt); } catch(e) {}
        if (pubD.listingId) console.log(`[smartSync] published OK listingId=${pubD.listingId}`);
        else if ((pubD.errors||[]).some(e => e.errorId === 25016))
          console.log(`[smartSync] publish skipped — some unmatched offers have $0 price (ok, offer updates already live)`);
        else if ((pubD.errors||[]).some(e => e.errorId === 25705)) {
          console.warn(`[smartSync] publish 25705 — group missing, trying individual offer publish`);
          // Try publishing each offer individually — bypasses the group requirement
          let indivOk = 0;
          for (const { offerId } of updates) {
            try {
              const ipr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${encodeURIComponent(offerId)}/publish`, {
                method: 'POST', headers: auth, body: JSON.stringify({}),
              });
              const iptxt = await ipr.text();
              if (ipr.ok || ipr.status === 200) { indivOk++; }
              else console.warn(`[smartSync] indiv publish ${offerId}: ${iptxt.slice(0,100)}`);
            } catch(e) {}
          }
          if (indivOk > 0) console.log(`[smartSync] individual publish: ${indivOk}/${updates.length} ok`);
          else {
            console.warn(`[smartSync] individual publish also failed — listing may need re-push`);
            return res.json({ success: false, error: 'eBay group deleted and individual publish failed', needsRepush: true, synced: okCount });
          }
        } else console.warn(`[smartSync] publish ${pubR.status}: ${pubTxt.slice(0,800)}`);
      } catch(e) { console.warn('[smartSync] publish error:', e.message); }

      return res.json({
        success: true, synced: okCount, failed: failCount,
        orphanedCount: _orphanedSkus.length,
        orphanedSkus:  _orphanedSkus.length > 0 ? _orphanedSkus.slice(0, 20) : undefined,
        fetchFailedCount: _fetchFailed.size,
        fetchFailedAsins: _fetchFailed.size > 0 ? [..._fetchFailed].slice(0, 20) : undefined,
        nextAsinOffset,
        totalAsins: allUniqueAsins.length,
        skuToAsin: Object.keys(skuToAsin).length > 0 ? skuToAsin : undefined,
        prices: Object.fromEntries(uniqueAsins.map(a => [a, {
          cost: asinPrice[a]||0,
          inStock: asinInStock[a]!==false,
          fetchFailed: _fetchFailed.has(a),
        }])),
        // Representative values for frontend display — use the MIN in-stock cost so the
        // Listed view shows the cheapest available variant (matches eBay's "starting at" UX).
        // These update p.price and p.myPrice on the product after each successful sync so
        // the stored display values can't go stale.
        appliedCost:  (() => {
          const inStockCosts = uniqueAsins
            .filter(a => asinInStock[a] !== false && asinPrice[a] > 0)
            .map(a => asinPrice[a]);
          if (inStockCosts.length) return Math.min(...inStockCosts);
          // Fallback: min of ALL non-zero costs (even OOS) so we have SOMETHING sensible
          const anyCosts = uniqueAsins.map(a => asinPrice[a]).filter(p => p > 0);
          return anyCosts.length ? Math.min(...anyCosts) : 0;
        })(),
        appliedPrice: (() => {
          // Exclude orphan force-OOS updates from the representative price calc —
          // orphans carry stale/wrong prices and would pollute the "starting at" value.
          const appliedPrices = updates
            .filter(u => !u._orphan)
            .map(u => parseFloat(u.price.value))
            .filter(p => p > 0);
          return appliedPrices.length ? Math.min(...appliedPrices) : 0;
        })(),
      });
    }

    if (action === 'sync') {
      // Strategy: identical to revise — full inventory item PUT (title+desc+images+aspects+qty)
      // + group PUT + offer price update. Returns a diff summary of what changed.
      const { access_token, ebaySku, sourceUrl } = body;
      if (!access_token || !ebaySku || !sourceUrl) {
        return res.status(400).json({ error: 'Missing access_token, ebaySku, or sourceUrl' });
      }
      const EBAY_API   = getEbayUrls().EBAY_API; // production only
      const markupPct  = parseFloat(body.markup ?? 0);
      const handling   = parseFloat(body.handlingCost ?? 2);
      const ebayFee    = 0.1335;
      const defaultQty = parseInt(body.quantity) || 1;
      const applyMk    = (cost) => {
        const c = parseFloat(cost) || 0;
        if (c <= 0) return 0;
        return Math.ceil(((c + handling) * (1 + markupPct / 100) / (1 - ebayFee) + 0.30) * 100) / 100;
      };
      const auth = {
        Authorization:      `Bearer ${access_token}`,
        'Content-Type':     'application/json',
        'Content-Language': 'en-US',
        'Accept-Language':  'en-US',
      };
      console.log(`[sync] sku=${ebaySku?.slice(0,35)} markup=${markupPct}%`);

      // ── STEP 1: Scrape source (Amazon / AliExpress / future) ─────────────────
      const _isAESync = /aliexpress\.com/i.test(sourceUrl || '');
      let product = null;

      if (_isAESync) {
        // AliExpress: try client HTML first, then server fetch
        const _aeIdSync = (sourceUrl||'').match(/\/item\/(\d+)\.html/)?.[1] || '';
        const _aeClientHtml = body.clientHtml || null;
        if (_aeClientHtml && _aeClientHtml.length > 10000) {
          product = aeScrapeProduct(_aeClientHtml, _aeIdSync);
        }
        if (!product?.price && _aeIdSync) {
          const _aeHdrs = { 'User-Agent': randUA(), 'Accept': 'text/html,*/*', 'Accept-Language': 'en-US,en;q=0.9',
            'Cookie': 'aep_usuc_f=site=glo&c_tp=USD&x_alilg=en&b_locale=en_US', 'Referer': 'https://www.aliexpress.com/' };
          try {
            const _aeR = await fetch(`https://www.aliexpress.com/item/${_aeIdSync}.html`, { headers: _aeHdrs, redirect: 'manual' });
            if (_aeR.status !== 301 && _aeR.status !== 302) {
              const _aeH = await _aeR.text();
              if (_aeH?.length > 10000) product = aeScrapeProduct(_aeH, _aeIdSync);
            }
          } catch(e) { console.warn('[sync] AliExpress fetch failed:', e.message); }
        }
        if (product) {
          if (!product.comboAsin)    product.comboAsin    = {};
          if (!product.comboInStock) product.comboInStock = {};
          if (!product.sizePrices)   product.sizePrices   = {};
          if (!product._source)      product._source      = 'aliexpress';
        }
      } else {
        // Amazon: call the scraper directly in-process (no HTTP, no self-call)
        try {
          product = await scrapeAmazonProduct(sourceUrl, body.clientHtml || null, body.clientAsinData || null);
        } catch (e) {
          console.warn('[sync] scrape failed:', e.message);
          product = null;
        }
      }

      // Fallback to cached frontend data if Amazon blocked
      const fallbackImages  = Array.isArray(body.fallbackImages) ? body.fallbackImages.filter(u => typeof u === 'string' && u.startsWith('http')) : [];
      const fallbackPrice   = parseFloat(body.fallbackPrice) || 0;
      const fallbackInStock = body.fallbackInStock !== false;
      const fallbackTitle   = typeof body.fallbackTitle === 'string' ? body.fallbackTitle : '';

      if (!product) {
        // Amazon blocked — use any cached data from frontend (images, price, title)
        if (fallbackTitle || fallbackPrice > 0 || fallbackImages.length) {
          console.log(`[sync] scrape blocked — using cached fallback: title="${fallbackTitle?.slice(0,40)}" imgs=${fallbackImages.length} price=$${fallbackPrice}`);
          product = {
            title: fallbackTitle || 'Product', price: fallbackPrice, images: fallbackImages,
            inStock: fallbackInStock, hasVariations: false, variations: [], variationImages: {},
            comboPrices: {}, sizePrices: {}, aspects: {}, breadcrumbs: [], bullets: [], descriptionPara: '',
          };
        }
      } else if (!product.images?.length && fallbackImages.length) {
        product.images = fallbackImages;
      }

      if (!product) {
        return res.status(400).json({ error: 'Amazon is blocking requests and no cached data is available. Open the product on the Import tab to refresh it first.' });
      }
      console.log(`[sync] scraped: "${product.title?.slice(0,50)}" imgs=${product.images.length} price=$${product.price} hasVar=${product.hasVariations}`);

      // ── STEP 1b: Zero-out all existing eBay inventory before populating ───────
      // Same as revise: Amazon is truth — zero everything first, then only confirmed
      // prices get qty=1. Prevents stale quantities from surviving a re-sync.
      {
        const _syncGrpRes = await fetch(
          `${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`,
          { headers: auth }
        ).catch(() => null);
        const _syncSkus = (_syncGrpRes?.ok ? await _syncGrpRes.json().catch(()=>({})) : {}).variantSKUs || [];
        if (_syncSkus.length) {
          // Batch-fetch offer IDs, then bulk-zero quantities
          const _syncOfferMap = {};
          for (let _oi = 0; _oi < _syncSkus.length; _oi += 20) {
            await Promise.all(_syncSkus.slice(_oi, _oi + 20).map(async sku => {
              try {
                const _or = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}`, { headers: auth });
                if (!_or.ok) return;
                const _od = await _or.json().catch(()=>({}));
                const _offerId = (_od.offers||[])[0]?.offerId;
                if (_offerId) _syncOfferMap[sku] = _offerId;
              } catch(e) {}
            }));
          }
          const _syncEntries = Object.entries(_syncOfferMap);
          let _syncZeroed = 0;
          for (let _zi = 0; _zi < _syncEntries.length; _zi += 25) {
            const batch = _syncEntries.slice(_zi, _zi + 25).map(([sku, offerId]) => ({
              offerId, availableQuantity: 0,
            }));
            const _br = await fetch(`${EBAY_API}/sell/inventory/v1/bulk_update_price_quantity`, {
              method: 'POST', headers: auth, body: JSON.stringify({ requests: batch }),
            }).catch(()=>null);
            if (_br?.ok) _syncZeroed += batch.length;
          }
          if (_syncZeroed) console.log(`[sync] step1b: zeroed ${_syncZeroed}/${_syncSkus.length} SKUs before Amazon fetch`);
        }
      }

      // ── STEP 2: AI category + title + aspects (same as push/revise) ─────────
      const suggestions = [];
      const ai          = null; // AI removed
      const categoryId  = suggestions[0]?.id || '11450';
      const listingTitle = sanitizeTitle(neutralizeTitle(product.ebayTitle || product.title || 'Product', product));

      // ── STEP 3: Build description (same as push/revise) ──────────────────────
      const buildEbayDesc = (title, bullets, para, asp) => {
        const bulletHtml = (bullets || []).length
          ? '<ul>' + bullets.map(b => `<li>${String(b).replace(/</g,'&lt;').replace(/>/g,'&gt;')}</li>`).join('') + '</ul>' : '';
        const specRows = Object.entries(asp || {})
          .filter(([k,v]) => !['ASIN','UPC','Color','Size','Brand Name','Brand'].includes(k) && v[0] && String(v[0]).length < 80)
          .slice(0, 10).map(([k,v]) => `<tr><td><b>${k}</b></td><td>${v[0]}</td></tr>`).join('');
        const specsTable = specRows
          ? `<br/><table border="0" cellpadding="4" cellspacing="0" width="100%"><tbody>${specRows}</tbody></table>` : '';
        return [`<h2>${title}</h2>`, bulletHtml, para ? `<p>${para}</p>` : '', specsTable,
          '<br/><p style="font-size:11px;color:#888">Ships from US. Item is new. Please message us with any questions before purchasing.</p>',
        ].filter(Boolean).join('\n');
      };
      const ebayDescription = buildEbayDesc(listingTitle, product.bullets || [], product.descriptionPara || '', product.aspects || '')
                           || product.description || listingTitle;

      const aspects = { ...(product.aspects || {}) };
      delete aspects['Color']; delete aspects['color'];
      delete aspects['Size'];  delete aspects['size'];

      // Fill required item specifics from product data
  {
    const _t = (product.title || '').toLowerCase();
    const _c = (product.breadcrumbs || []).join(' ').toLowerCase();
    const _isCloth = /shirt|pant|dress|top|legging|hoodie|sweat|jacket|coat|bra|sock|jean|blouse|vest|skirt|suit|scarf|hat|cap|beanie|glove|boot|shoe|sneaker|sandal|\bshort\b|underwear|thong|brief|lingerie|swimsuit|bikini|activewear|athletic.*wear/i.test(_t);
    const _isMen = /\bmen\b|\bmale\b/i.test(_t + ' ' + _c);
    const _isKid = /\bkid\b|\bchild\b|\btoddler\b/i.test(_t + ' ' + _c);
    if (!aspects['Brand']) aspects['Brand'] = ['Unbranded'];
    if (!aspects['Type'])  aspects['Type']  = ['Other'];
    if (_isCloth) {
      if (!aspects['Department'])  aspects['Department']  = [_isKid ? 'Kids/Boys' : (_isMen ? 'Men' : 'Women')];
      if (!aspects['Size Type'])   aspects['Size Type']   = ['Regular'];
      if (!aspects['Material']) {
        // Extract material from title/breadcrumbs
        const _mat = (_t + ' ' + _c).toLowerCase();
        if (/stainless.steel|steel/i.test(_mat)) aspects['Material'] = ['Stainless Steel'];
        else if (/aluminum|aluminium/i.test(_mat)) aspects['Material'] = ['Aluminum'];
        else if (/plastic|acrylic/i.test(_mat)) aspects['Material'] = ['Plastic'];
        else if (/cotton/i.test(_mat)) aspects['Material'] = ['Cotton'];
        else if (/polyester/i.test(_mat)) aspects['Material'] = ['Polyester'];
        else if (/nylon/i.test(_mat)) aspects['Material'] = ['Nylon'];
        else if (/spandex|lycra/i.test(_mat)) aspects['Material'] = ['Spandex'];
        else if (/wool/i.test(_mat)) aspects['Material'] = ['Wool'];
        else if (/leather/i.test(_mat)) aspects['Material'] = ['Leather'];
        else if (/silicone/i.test(_mat)) aspects['Material'] = ['Silicone'];
        else if (/rubber/i.test(_mat)) aspects['Material'] = ['Rubber'];
        else if (/wood|bamboo/i.test(_mat)) aspects['Material'] = ['Wood'];
        else if (/glass/i.test(_mat)) aspects['Material'] = ['Glass'];
        else if (/canvas/i.test(_mat)) aspects['Material'] = ['Canvas'];
        else if (/foam/i.test(_mat)) aspects['Material'] = ['Foam'];
        else if (/mesh/i.test(_mat)) aspects['Material'] = ['Mesh'];
        else if (/vinyl|pvc/i.test(_mat)) aspects['Material'] = ['Vinyl'];
        else aspects['Material'] = ['Synthetic'];
      }
      if (!aspects['Occasion'])    aspects['Occasion']    = ['Casual'];
      if (!aspects['Pattern'])     aspects['Pattern']     = ['Solid'];
      if (!aspects['Style']) {
        if (/bra\b|bralette|wireless.*bra|sports.*bra|strapless/i.test(_t)) aspects['Style'] = ['Wireless'];
        else if (/brief|boyshort|thong|panty|panties|bikini.*bottom/i.test(_t)) aspects['Style'] = ['Brief'];
        else if (/boxer|trunk/i.test(_t)) aspects['Style'] = ['Boxer'];
        else if (/bikini|swimsuit|one.piece/i.test(_t)) aspects['Style'] = ['Bikini'];
        else aspects['Style'] = ['Other'];
      }
      if (!aspects['Bra Size']) {
        if (/bra\b|bralette|wireless.*bra|sports.*bra/i.test(_t)) aspects['Bra Size'] = ['M'];
      }
    }
    // Furniture/sets
    const _isFurn = /dining.*set|bedroom.*set|sofa.*set|sectional|furniture.*set|piece.*set/i.test(_t);
    if (_isFurn) {
      if (!aspects['Set Includes']) {
        const pm = _t.match(/(\d+)\s*(?:piece|pc)/i);
        const n = pm ? parseInt(pm[1]) : 1;
        if (/dining/i.test(_t)) aspects['Set Includes'] = [`${Math.max(n-1,1)} Chair${n>2?'s':''}, Table`];
        else if (/bedroom/i.test(_t)) aspects['Set Includes'] = ['Bed Frame, Nightstand, Dresser'];
        else if (/sofa|sectional/i.test(_t)) aspects['Set Includes'] = ['Sofa'];
        else aspects['Set Includes'] = [`${n} Piece Set`];
      }
      if (!aspects['Number of Items in Set']) {
        const pm2 = _t.match(/(\d+)\s*(?:piece|pc|pack)/i);
        aspects['Number of Items in Set'] = [pm2 ? pm2[1] : '1'];
      }
      if (!aspects['Number of Pieces']) {
        const pm3 = _t.match(/(\d+)\s*(?:piece|pc)/i);
        if (pm3) aspects['Number of Pieces'] = [pm3[1]];
      }
      if (!aspects['Room']) {
        if (/dining/i.test(_t)) aspects['Room'] = ['Dining Room'];
        else if (/bedroom/i.test(_t)) aspects['Room'] = ['Bedroom'];
        else if (/living/i.test(_t)) aspects['Room'] = ['Living Room'];
        else if (/outdoor|patio/i.test(_t)) aspects['Room'] = ['Patio/Garden'];
        else if (/office/i.test(_t)) aspects['Room'] = ['Office'];
        else aspects['Room'] = ['Any Room'];
      }
      if (!aspects['Assembly Required']) aspects['Assembly Required'] = ['Yes'];
    }
    // Mattress / bedding specifics
    const _isMattress = /mattress|mattresses/i.test(_t);
    if (_isMattress) {
      if (!aspects['Firmness']) {
        if (/firm(?!ness)/i.test(_t)) aspects['Firmness'] = ['Firm'];
        else if (/soft/i.test(_t)) aspects['Firmness'] = ['Soft'];
        else if (/plush/i.test(_t)) aspects['Firmness'] = ['Medium Plush'];
        else if (/medium/i.test(_t)) aspects['Firmness'] = ['Medium'];
        else aspects['Firmness'] = ['Medium'];
      }
      if (!aspects['Type']) {
        if (/memory.foam/i.test(_t)) aspects['Type'] = ['Memory Foam'];
        else if (/innerspring|spring/i.test(_t)) aspects['Type'] = ['Innerspring'];
        else if (/hybrid/i.test(_t)) aspects['Type'] = ['Hybrid'];
        else if (/latex/i.test(_t)) aspects['Type'] = ['Latex'];
        else aspects['Type'] = ['Memory Foam'];
      }
      if (!aspects['Thickness']) {
        const thickM = _t.match(/(\d{1,2})\s*(?:inch|in\.?|")/i);
        if (thickM) aspects['Thickness'] = [thickM[1] + ' in'];
      }
      if (!aspects['Features']) {
        aspects['Features'] = ['CertiPUR-US Certified'];
      }
    }
    // Weight vest / fitness
    if (/weighted.vest|weight.vest/i.test(_t)) {
      if (!aspects['Weight']) {
        const wM = _t.match(/(\d+)\s*(?:lb|pound|kg)/i);
        if (wM) aspects['Weight'] = [wM[1] + (/ kg/i.test(_t) ? ' kg' : ' lb')];
      }
    }
    // Department for non-clothing items (bags, accessories, etc.)
    if (!aspects['Department'] && !_isCloth) {
      if (/\bwomen\b|\bfemale\b|\bladies\b/i.test(_t + ' ' + _c)) aspects['Department'] = ['Women'];
      else if (/\bmen\b|\bmale\b/i.test(_t + ' ' + _c)) aspects['Department'] = ['Men'];
    }
    // Dimension aspects for storage/organizer products
    if (!aspects['Product']) {
      // Only add if category might need it
      if (/organizer|storage|shelf|rack|caddy|holder/i.test(_t)) aspects['Product'] = [(_t).slice(0,65)];
    // Dimension aspects — extract from title or use category-appropriate defaults
    // Many kitchen/storage/home categories require these
    const _extractDim = (title, units=['inch','in\.?','"','cm']) => {
      const pat = new RegExp('(\\d+(?:\\.\\d+)?)\\s*(?:x\\s*\\d+(?:\\.\\d+)?\\s*x\\s*\\d+(?:\\.\\d+)?\\s*)?(?:' + units.join('|') + ')', 'i');
      const m = title.match(pat);
      return m ? m[1] + ' in' : null;
    };
    if (!aspects['Item Width'] && !aspects['Item Length'] && !aspects['Item Height']) {
      // Only auto-add for categories that commonly require dimensions
      if (/rack|drying|organizer|shelf|basket|bin|tray|caddy|drawer|cabinet|stand|holder|board|mat/i.test(_t)) {
        const dim = _extractDim(_t);
        if (!aspects['Item Width'])  aspects['Item Width']  = [dim || 'See Description'];
        if (!aspects['Item Length']) aspects['Item Length'] = [dim || 'See Description'];
        if (!aspects['Item Height']) aspects['Item Height'] = ['See Description'];
      }
    }
    }
    if (!aspects['Country/Region of Manufacture']) aspects['Country/Region of Manufacture'] = ['China'];
    // Truncate all aspect values to eBay's 65-char limit
    for (const [k, v] of Object.entries(aspects)) {
      if (Array.isArray(v)) aspects[k] = v.map(s => String(s).slice(0, 65));
    }
    console.log('[push] aspects filled:', Object.keys(aspects).length, 'fields');

    // eBay hard limit: max 45 item specifics per listing
    // Priority: keep required/high-value fields, drop low-value extras
    if (Object.keys(aspects).length > 45) {
      const PRIORITY_KEEP = new Set(['Brand','Color','Size','Material','Style','Department','Type',
        'Pattern','Occasion','Size Type','Country/Region of Manufacture','MPN','UPC','EAN','ISBN',
        'Set Includes','Number of Items in Set','Room','Assembly Required','Theme','Age Group',
        'Gender','Fabric Type','Care Instructions','Fit','Neckline','Sleeve Length','Closure',
        'Features','Compatible Model','Compatible Brand','Connectivity','Wattage','Voltage',
        'Capacity','Shape','Seating Capacity','Number of Pieces','Bra Size',
        'Chest Size','Waist Size','Inseam','Leg Style']);
      const keys = Object.keys(aspects);
      const keep = keys.filter(k => PRIORITY_KEEP.has(k));
      const extra = keys.filter(k => !PRIORITY_KEEP.has(k));
      // Fill up to 45 with priority first, then extras
      const allowed = new Set([...keep, ...extra].slice(0, 45));
      for (const k of keys) { if (!allowed.has(k)) delete aspects[k]; }
      console.log(`[push] aspects capped from ${keys.length} to ${Object.keys(aspects).length} (eBay limit 45)`);
    }
  }
  // Truncate ALL aspect values to 65 chars max (eBay hard limit)
  for (const k of Object.keys(aspects)) {
    if (Array.isArray(aspects[k])) {
      aspects[k] = aspects[k].map(v => typeof v === 'string' && v.length > 65 ? v.slice(0, 62) + '...' : v);
    }
  }

      const freshStock  = product.inStock !== false;
      const colorGroup  = product.variations?.find(v => /color|colour/i.test(v.name));
      const sizeGroup   = product.variations?.find(v => /size/i.test(v.name));
      const colorImgs   = product.variationImages?.['Color'] || {};
      const comboPrices = product.comboPrices || {};
      const sizePrices  = product.sizePrices  || {};
      const comboAsin   = product.comboAsin   || {};
      const comboInStock = product.comboInStock || {};
      const basePrice   = parseFloat(product.price || 0);

      // Per-variant price — exact same logic as push/revise
      const getPriceForVariant = (color, size) => {
        const key = `${color||''}|${size||''}`;
        const cv  = colorGroup?.values?.find(v => v.value === color);
        const sv  = sizeGroup?.values?.find(v => v.value === size);
        const amazonPrice = comboPrices[key] || 0; // each combo has unique price, no fallback
        const p = applyMk(amazonPrice);
        return p > 0 ? p : 0;
      };

      // Per-variant qty — combo must exist AND be in stock on Amazon.
      // CRITICAL: we do NOT gate this on the global `freshStock` (product.inStock)
      // flag — that flag reflects the LANDING variant's buy box only. For variation
      // products, if Amazon lands us on Chocolate 8'x10' and that specific variant
      // is temporarily OOS, `product.inStock` comes back false, and the old code
      // zeroed EVERY variant regardless of individual stock state. That was the
      // "Deep Rebuild ended my listing" bug — all 23 LIANLAM rug variants went to
      // qty=0 even though only the landing one was OOS. Per-variant data
      // (comboAsin/comboInStock/comboPrices) already tells us each variant's
      // real state; trust it.
      const getQtyForVariant = (color, size) => {
        if (Object.keys(comboAsin).length) {
          const key = `${color||''}|${size||''}`;
          if (!comboAsin[key]) return 0;
          if (comboInStock[key] === false) return 0;
          // No price from Amazon → qty 0
          const hasPrice = comboPrices[key] > 0; // only combo price counts
          if (!hasPrice) return 0;
          return defaultQty;
        }
        // Single-variant (no comboAsin): need a base price AND landing-variant in-stock
        if (!freshStock) return 0;
        const hasBasePrice = parseFloat(product.price || product.cost || 0) > 0;
        return hasBasePrice ? defaultQty : 0;
      };

      // Per-color image — same as push/revise
      const getImageForColor = (color) =>
        (color ? colorImgs[color] : null) || product.images[0] || '';

      console.log(`[sync] comboPrices=${Object.keys(comboPrices).length} sizePrices=${Object.keys(sizePrices).length} comboAsin=${Object.keys(comboAsin).length}`);

      // ── STEP 4: Get existing variant SKUs from eBay ──────────────────────────
      // SAFETY: this decision (variation vs simple) must be BULLETPROOF. If we
      // can't confirm the group's state with high confidence, we MUST NOT fall
      // through to the simple branch silently — doing so has caused listings to
      // end accidentally when combined with the Deep Rebuild mismatch prompt.
      // The rules:
      //   1. If the GET succeeds AND returns variantSKUs[] non-empty → variation
      //   2. If the GET returns 404 (no group) AND body.hasVariations is false → simple
      //   3. Any other case (HTTP error, empty body, missing variantSKUs while
      //      body.hasVariations=true, JSON parse failure, transient 5xx, etc.)
      //      → ABORT with a clear error so the frontend never misinterprets
      //      this as a simple listing and triggers a destructive re-push.
      let _groupResp = null;
      let _groupJson = null;
      let _groupFetchErr = null;
      try {
        _groupResp = await fetch(
          `${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`,
          { headers: auth }
        );
        if (_groupResp.ok) {
          _groupJson = await _groupResp.json().catch(e => { _groupFetchErr = 'json-parse'; return null; });
        } else if (_groupResp.status !== 404) {
          _groupFetchErr = `http-${_groupResp.status}`;
        }
      } catch(e) {
        _groupFetchErr = 'network-' + (e.message || 'unknown');
      }

      const variantSkus = _groupJson?.variantSKUs || [];
      const _clientHadVariations = body.hasVariations === true || (Array.isArray(body.fallbackVariations) && body.fallbackVariations.length > 0);

      // Decision matrix:
      let isVariation;
      if (_groupFetchErr) {
        // Unknown state — refuse to guess. Return an error with a clear code so
        // the frontend can surface it without firing any destructive follow-up.
        console.warn(`[sync] ABORT — inventory_item_group GET uncertain: ${_groupFetchErr}`);
        return res.status(502).json({
          success: false,
          error: `Cannot determine listing type from eBay — ${_groupFetchErr}. Retry in a moment. No changes made to the listing.`,
          code:   'GROUP_UNKNOWN',
          groupFetchErr: _groupFetchErr,
        });
      } else if (variantSkus.length > 0) {
        isVariation = true;
      } else if (_groupResp?.status === 404 && !_clientHadVariations) {
        // Group not found and client confirmed no variations → legitimate simple listing
        isVariation = false;
      } else if (_clientHadVariations) {
        // eBay says "no variants" (or 404) but the client passed hasVariations=true.
        // This is the Deep Rebuild trigger case — never silently fall through to
        // simple. Instead, abort with a distinctive code so the frontend can
        // show a specific warning without calling doRepush.
        console.warn(`[sync] ABORT — eBay has no variants (status=${_groupResp?.status}) but client claims hasVariations=true. Refusing to treat as simple.`);
        return res.status(409).json({
          success: false,
          error:   `eBay listing has no variant group but Amazon product has variations. ` +
                   `This is likely an inventory_item_group drift — the listing may need manual inspection. ` +
                   `No changes were made.`,
          code:    'VARIATION_STATE_DRIFT',
          ebayGroupStatus: _groupResp?.status,
          ebayVariantCount: variantSkus.length,
          clientHasVariations: true,
        });
      } else {
        isVariation = false;
      }
      console.log(`[sync] type=${isVariation?'variation':'simple'} variantSkus=${variantSkus.length}${_groupFetchErr?` (err=${_groupFetchErr})`:''}`);
      const groupRes = _groupJson; // back-compat alias for any later reference

      // Change tracking
      const priceChanges = [], stockChanges = [], imageChanges = [];

      if (isVariation) {
        // ── VARIATION: full inventory PUT per variant (same as push/revise) ────

        // Read Color+Size aspects for all existing variant SKUs from eBay
        const skuAspects = {};  // sku → { Color, Size }
        for (let i = 0; i < variantSkus.length; i += 20) {
          const batch = variantSkus.slice(i, i + 20);
          const qs    = batch.map(s => `sku=${encodeURIComponent(s)}`).join('&');
          const bd    = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item?${qs}`, { headers: auth })
            .then(r => r.json()).catch(() => ({}));
          for (const item of (bd.inventoryItems || [])) {
            const asp = item.inventoryItem?.product?.aspects || {};
            skuAspects[item.sku] = {
              Color: (asp['Color'] || asp['color'] || [])[0] || null,
              Size:  (asp['Size']  || asp['size']  || [])[0] || null,
            };
          }
          if (i + 20 < variantSkus.length) await sleep(100);
        }
        console.log(`[sync] aspects read: ${Object.keys(skuAspects).length}/${variantSkus.length}`);

        // Full inventory PUT per variant — same as push/revise
        // title + description + imageUrls (per-color) + aspects + qty
        const createdSkus = new Set();
        const failedSkus  = [];

        // Test first variant for 401
        if (variantSkus.length > 0) {
          const testSku = variantSkus[0];
          const { Color: testColor, Size: testSize } = skuAspects[testSku] || {};
          const testAsp = { ...aspects };
          if (testColor) testAsp['Color'] = [testColor];
          if (testSize)  testAsp['Size']  = [testSize];
          const testImg = getImageForColor(testColor);

          // Track old qty for stock change detection
          const oldItem = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(testSku)}`, { headers: auth })
            .then(r => r.ok ? r.json() : {}).catch(() => ({}));
          const oldQty  = oldItem?.availability?.shipToLocationAvailability?.quantity ?? -1;
          const newQty  = getQtyForVariant(testColor, testSize);

          const testR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(testSku)}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({
              availability: { shipToLocationAvailability: { quantity: newQty } },
              condition: 'NEW',
              product: {
                title:       listingTitle,
                description: ebayDescription,
                imageUrls:   [testImg, ...product.images.filter(x => x !== testImg)].filter(Boolean).slice(0, 12),
                aspects:     testAsp,
              },
            }),
          });
          if (testR.status === 401) {
            return res.status(401).json({ error: 'eBay token missing inventory permission. Go to Settings → Force Reconnect (fix 401) and re-authorize.', code: 'INVENTORY_401' });
          }
          if (testR.ok || testR.status === 204) {
            createdSkus.add(testSku);
            if (oldQty >= 0 && oldQty !== newQty) {
              const label = [testColor, testSize].filter(Boolean).join('/') || testSku.slice(-12);
              stockChanges.push(`${label}: qty ${oldQty}→${newQty}`);
              console.log(`[sync] stock ${label}: ${oldQty}→${newQty}`);
            }
          } else {
            const t = await testR.text();
            console.warn(`[sync] test PUT fail: ${testR.status} ${t.slice(0,100)}`);
            failedSkus.push(testSku);
          }
        }

        // Remaining variants in batches of 8 (same as push/revise)
        for (let i = 1; i < variantSkus.length; i += 8) {
          await Promise.all(variantSkus.slice(i, i + 8).map(async (sku) => {
            const { Color: color, Size: size } = skuAspects[sku] || {};
            const asp = { ...aspects };
            if (color) asp['Color'] = [color];
            if (size)  asp['Size']  = [size];
            const img    = getImageForColor(color);
            const newQty = getQtyForVariant(color, size);

            // Track old qty
            const oldItem = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, { headers: auth })
              .then(r => r.ok ? r.json() : {}).catch(() => ({}));
            const oldQty = oldItem?.availability?.shipToLocationAvailability?.quantity ?? -1;

            const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
              method: 'PUT', headers: auth,
              body: JSON.stringify({
                availability: { shipToLocationAvailability: { quantity: newQty } },
                condition: 'NEW',
                product: {
                  title:       listingTitle,
                  description: ebayDescription,
                  imageUrls:   img ? [img] : product.images.slice(0, 1),
                  aspects:     asp,
                },
              }),
            });
            if (r.ok || r.status === 204) {
              createdSkus.add(sku);
              if (oldQty >= 0 && oldQty !== newQty) {
                const label = [color, size].filter(Boolean).join('/') || sku.slice(-12);
                stockChanges.push(`${label}: qty ${oldQty}→${newQty}`);
              }
            } else {
              const t = await r.text();
              console.warn(`[sync] PUT fail ${sku.slice(-20)}: ${r.status} ${t.slice(0,80)}`);
              failedSkus.push(sku);
            }
          }));
          if (i + 8 < variantSkus.length) await sleep(150);
        }

        // Retry failed items once
        if (failedSkus.length) {
          await sleep(800);
          for (const sku of failedSkus) {
            const { Color: color, Size: size } = skuAspects[sku] || {};
            const asp = { ...aspects }; if (color) asp['Color'] = [color]; if (size) asp['Size'] = [size];
            const img    = getImageForColor(color);
            const newQty = getQtyForVariant(color, size);
            const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
              method: 'PUT', headers: auth,
              body: JSON.stringify({
                availability: { shipToLocationAvailability: { quantity: newQty } },
                condition: 'NEW',
                product: { title: listingTitle, description: ebayDescription,
                           imageUrls: [img, ...product.images.filter(x => x !== img)].filter(Boolean).slice(0, 12), aspects: asp },
              }),
            });
            if (r.ok || r.status === 204) createdSkus.add(sku);
          }
        }
        console.log(`[sync] inventory items: ${createdSkus.size}/${variantSkus.length} updated, ${failedSkus.length} failed`);

        // ── GROUP PUT: fresh images + variesBy (same as push/revise) ───────────
        const varAspects = {};
        if (colorGroup) varAspects['Color'] = colorGroup.values.map(v => v.value);
        if (sizeGroup)  varAspects['Size']  = sizeGroup.values.map(v => v.value);
        const colorUrlList   = Object.values(colorImgs).filter(Boolean).slice(0, 12);
        const groupImageUrls = colorUrlList.length ? colorUrlList : product.images.slice(0, 12);
        const groupAspects   = { ...aspects };
        for (const vg of (product.variations || [])) { delete groupAspects[vg.name]; delete groupAspects[vg.name.toLowerCase()]; }
        const variesBySpecs  = Object.entries(varAspects).map(([name, values]) => ({ name, values }));

        // Track group image changes
        const prevGroupImages = groupRes?.imageUrls || [];
        if (prevGroupImages.length !== groupImageUrls.length) {
          imageChanges.push(`Gallery: ${prevGroupImages.length}→${groupImageUrls.length} images`);
        }

        for (let attempt = 1; attempt <= 3; attempt++) {
          const gr = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({
              inventoryItemGroupKey: ebaySku,
              title:       listingTitle,
              description: ebayDescription,
              imageUrls:   groupImageUrls,
              variantSKUs: variantSkus.filter(s => createdSkus.has(s)),
              aspects:     groupAspects,
              variesBy: {
                aspectsImageVariesBy: colorGroup ? ['Color'] : [],
                specifications: variesBySpecs,
              },
            }),
          });
          if (gr.ok || gr.status === 204) { console.log('[sync] group PUT ok'); break; }
          const gt = await gr.text();
          console.warn(`[sync] group attempt ${attempt}: ${gr.status} ${gt.slice(0,200)}`);
          if (attempt < 3) await sleep(600);
        }

        // ── UPDATE OFFER PRICES: per-variant with correct color+size price ───
        let pricesUpdated = 0;
        for (let i = 0; i < variantSkus.length; i += 15) {
          await Promise.all(variantSkus.slice(i, i + 8).map(async (sku) => {
            if (!createdSkus.has(sku)) return;
            const { Color: color, Size: size } = skuAspects[sku] || {};
            const newPrice = getPriceForVariant(color, size);
            const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}`, { headers: auth })
              .then(r => r.json()).catch(() => ({}));
            const offer = (ol.offers || [])[0];
            if (offer?.offerId) {
              const oldPrice = parseFloat(offer.pricingSummary?.price?.value || 0);
              if (Math.abs(newPrice - oldPrice) > 0.01) {
                await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offer.offerId}`, {
                  method: 'PUT', headers: auth,
                  body: JSON.stringify({ pricingSummary: { price: { value: newPrice.toFixed(2), currency: 'USD' } } }),
                }).catch(() => {});
                const label = [color, size].filter(Boolean).join('/') || sku.slice(-12);
                priceChanges.push(`${label}: $${oldPrice.toFixed(2)}→$${newPrice.toFixed(2)}`);
                console.log(`[sync] price ${label}: $${oldPrice.toFixed(2)}→$${newPrice.toFixed(2)}`);
              }
              pricesUpdated++;
            }
          }));
          if (i + 15 < variantSkus.length) await sleep(80);
        }

        console.log(`[sync] done — ${createdSkus.size} updated, ${pricesUpdated} prices checked, ${priceChanges.length} price changes, ${stockChanges.length} stock changes`);
        return res.json({
          success:      true,
          type:         'variation',
          updatedVariants: createdSkus.size,
          failed:       failedSkus.length,
          images:       product.images.length,
          price:        applyMk(basePrice),
          inStock:      freshStock,
          priceChanges,
          stockChanges,
          imageChanges,
        });

      } else {
        // ── SIMPLE LISTING: full inventory PUT (same as push/revise) ──────────
        // Price selection: if the URL's ASIN has a matching combo entry, use THAT
        // price (so a simple listing of B01M14RQVX gets B01M14RQVX's own price,
        // not the cheapest sibling variant). Else fall back to product.price.
        let _simplePriceCost = parseFloat(product.price) || 0;
        const _urlAsin = (sourceUrl||'').match(/\/(?:dp|gp\/product)\/([A-Z0-9]{10})/)?.[1];
        if (_urlAsin && Object.keys(comboAsin).length) {
          for (const [k, a] of Object.entries(comboAsin)) {
            if (a === _urlAsin) {
              const cp = comboPrices[k];
              if (cp > 0) { _simplePriceCost = cp; console.log(`[sync/simple] using URL ASIN ${_urlAsin} price: $${cp} (overriding product.price=$${product.price})`); break; }
            }
          }
        }
        const newPrice = applyMk(_simplePriceCost) || 0;
        const newQty   = freshStock ? defaultQty : 0;

        // Track old values
        const oldItem  = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`, { headers: auth })
          .then(r => r.ok ? r.json() : {}).catch(() => ({}));
        const oldQty   = oldItem?.availability?.shipToLocationAvailability?.quantity ?? -1;
        const oldImgCount = (oldItem?.product?.imageUrls || []).length;

        const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: newQty } },
            condition: 'NEW',
            product: { title: listingTitle, description: ebayDescription, imageUrls: product.images.slice(0, 12), aspects },
          }),
        });
        if (ir.status === 401) {
          return res.status(401).json({ error: 'eBay token missing inventory permission. Go to Settings → Force Reconnect (fix 401) and re-authorize.', code: 'INVENTORY_401' });
        }
        if (!ir.ok) {
          const t = await ir.text();
          return res.status(400).json({ error: `Inventory update failed: ${t.slice(0,200)}` });
        }

        if (oldQty >= 0 && oldQty !== newQty) {
          stockChanges.push(`qty ${oldQty}→${newQty}`);
          console.log(`[sync/simple] stock: ${oldQty}→${newQty}`);
        }
        if (oldImgCount !== product.images.length) {
          imageChanges.push(`Images: ${oldImgCount}→${product.images.length}`);
        }

        // Update offer price
        const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(ebaySku)}`, { headers: auth })
          .then(r => r.json()).catch(() => ({}));
        const offer = (ol.offers || [])[0];
        if (offer?.offerId) {
          const oldPrice = parseFloat(offer.pricingSummary?.price?.value || 0);
          if (Math.abs(newPrice - oldPrice) > 0.01) {
            await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offer.offerId}`, {
              method: 'PUT', headers: auth,
              body: JSON.stringify({ pricingSummary: { price: { value: newPrice.toFixed(2), currency: 'USD' } } }),
            }).catch(() => {});
            priceChanges.push(`$${oldPrice.toFixed(2)} → $${newPrice.toFixed(2)}`);
            console.log(`[sync/simple] price: $${oldPrice.toFixed(2)} → $${newPrice.toFixed(2)}`);
          }
        }

        console.log(`[sync/simple] title="${listingTitle.slice(0,40)}" price=$${newPrice} qty=${newQty} imgs=${product.images.length}`);
        return res.json({
          success:      true,
          type:         'simple',
          updatedVariants: 1,
          images:       product.images.length,
          price:        newPrice,
          inStock:      freshStock,
          priceChanges,
          stockChanges,
          imageChanges,
        });
      }
    }

    // ── END LISTING ───────────────────────────────────────────────────────────
    // ── DEALS SCRAPE v2: get ASINs from Amazon Today's Deals page ────────────────
    if (action === 'bulkScrapeAsins') action = 'dealsScrape'; // backward compat alias
    if (action === 'dealsScrape') {
      const { exclude = [], count = 25, dept = null, customUrls = null } = body;
      const excludeSet = new Set(exclude);

      // VeRO / brand-protected items to skip (eBay will reject these)
      const VERO_BRANDS = new Set([
        'samsung','apple','nike','adidas','sony','microsoft','lg','bose',
        'beats','dyson','lego','gucci','louis vuitton','chanel','rolex',
        'prada','versace','burberry','yeezy','jordan','off-white','supreme',
        'north face','ugg','timberland','new balance','under armour','reebok',
        'puma','vans','converse','dr. martens','crocs','birkenstock',
        'kindle','echo','ring','roomba','nespresso','keurig','instant pot',
        'yale','schlage','kwikset','defiant','master lock','august lock','smartlock',
        'nintendo','playstation','xbox','fitbit','garmin','gopro',
        'turbotax','doordash','uber','instacart','grubhub',
        'pokemon','disney','marvel','dc comics','star wars','harry potter',
      ]);

      // Digital / non-physical product keywords to skip
      const SKIP_KEYWORDS = [
        'ebook','kindle edition','gift card','egift','digital code',
        'game download','app purchase','audible','prime video',
        'rolling balls','brain games','alpha\'s bride','design home',
        'iq boost','training brain','free 3d game',
      ];

      const isVero = (title, brand) => {
        const t = (title + ' ' + (brand||'')).toLowerCase();
        for (const b of VERO_BRANDS) if (t.includes(b)) return true;
        for (const k of SKIP_KEYWORDS) if (t.includes(k)) return true;
        return false;
      };

      // Scrape multiple Amazon pages to get a large, diverse ASIN pool
      // Organized by department for broad inventory coverage
      const AMAZON_PAGES_BY_DEPT = {
        deals: [
          'https://www.amazon.com/deals?ref_=nav_cs_gb',
          'https://www.amazon.com',
        ],
        electronics: [
          'https://www.amazon.com/b?node=172282',
          'https://www.amazon.com/gp/bestsellers/electronics',
          'https://www.amazon.com/s?k=phone+cases&i=electronics',
          'https://www.amazon.com/s?k=wireless+earbuds&i=electronics',
          'https://www.amazon.com/s?k=portable+charger&i=electronics',
          'https://www.amazon.com/s?k=smart+watch&i=electronics',
          'https://www.amazon.com/s?k=ring+light+tripod&i=electronics',
          'https://www.amazon.com/s?k=usb+accessories&i=electronics',
        ],
        tools_diy: [
          'https://www.amazon.com/b?node=228013',
          'https://www.amazon.com/gp/bestsellers/tools',
          'https://www.amazon.com/s?k=tool+set+home&i=tools',
          'https://www.amazon.com/s?k=led+strip+lights&i=tools',
          'https://www.amazon.com/s?k=smart+home+accessories&i=tools',
          'https://www.amazon.com/s?k=storage+bins+shelves&i=tools',
        ],
        home_garden: [
          'https://www.amazon.com/b?node=1055398',
          'https://www.amazon.com/b?node=2972638011',
          'https://www.amazon.com/gp/bestsellers/garden',
          'https://www.amazon.com/s?k=area+rugs&i=garden',
          'https://www.amazon.com/s?k=throw+pillows&i=garden',
          'https://www.amazon.com/s?k=bedding+comforter+sets&i=garden',
          'https://www.amazon.com/s?k=curtains+window&i=garden',
          'https://www.amazon.com/s?k=artificial+plants+flowers&i=garden',
          'https://www.amazon.com/s?k=outdoor+patio+furniture&i=garden',
          'https://www.amazon.com/s?k=bathroom+accessories&i=garden',
          'https://www.amazon.com/s?k=wall+art+canvas&i=garden',
          'https://www.amazon.com/s?k=candles+home+decor&i=garden',
        ],
        kitchen: [
          'https://www.amazon.com/b?node=1055398',
          'https://www.amazon.com/gp/bestsellers/kitchen',
          'https://www.amazon.com/s?k=kitchen+gadgets&i=kitchen',
          'https://www.amazon.com/s?k=food+storage+containers&i=kitchen',
          'https://www.amazon.com/s?k=coffee+mugs+tumblers&i=kitchen',
          'https://www.amazon.com/s?k=dish+drying+rack&i=kitchen',
          'https://www.amazon.com/s?k=kitchen+organizer&i=kitchen',
          'https://www.amazon.com/s?k=cooking+utensils+set&i=kitchen',
        ],
        office: [
          'https://www.amazon.com/b?node=1064954',
          'https://www.amazon.com/gp/bestsellers/office-products',
          'https://www.amazon.com/s?k=desk+organizer&i=office-products',
          'https://www.amazon.com/s?k=planner+notebook+journal&i=office-products',
          'https://www.amazon.com/s?k=desk+lamp+led&i=office-products',
          'https://www.amazon.com/s?k=monitor+stand+laptop+riser&i=office-products',
        ],
        baby: [
          'https://www.amazon.com/b?node=165796011',
          'https://www.amazon.com/s?ref=nb_sb_noss_2&url=search-alias%3Dfashion-baby&field-keywords=',
          'https://www.amazon.com/gp/bestsellers/baby-products',
          'https://www.amazon.com/s?k=baby+clothes+newborn&i=baby-products',
          'https://www.amazon.com/s?k=baby+toys&i=baby-products',
          'https://www.amazon.com/s?k=diaper+bag+backpack&i=baby-products',
          'https://www.amazon.com/s?k=baby+bath+essentials&i=baby-products',
        ],
        fashion_kids: [
          'https://www.amazon.com/s?ref=nb_sb_noss_2&url=search-alias%3Dfashion-boys&field-keywords=',
          'https://www.amazon.com/s?ref=nb_sb_noss?url=search-alias%3Dfashion-girls&field-keywords=',
          'https://www.amazon.com/gp/bestsellers/fashion/1046764',
          'https://www.amazon.com/s?k=girls+dresses&i=fashion-girls',
          'https://www.amazon.com/s?k=boys+athletic+shorts&i=fashion-boys',
          'https://www.amazon.com/s?k=kids+school+uniforms&i=fashion-girls',
        ],
        fashion_women: [
          'https://www.amazon.com/b?node=7147440011',
          'https://www.amazon.com/gp/bestsellers/fashion/2475809011',
          'https://www.amazon.com/s?k=women+tops+blouses&i=fashion-womens',
          'https://www.amazon.com/s?k=women+leggings+activewear&i=fashion-womens',
          'https://www.amazon.com/s?k=women+dresses&i=fashion-womens',
          'https://www.amazon.com/s?k=women+shorts&i=fashion-womens',
          'https://www.amazon.com/s?k=women+swimsuit&i=fashion-womens',
          'https://www.amazon.com/s?k=women+pajamas+sleepwear&i=fashion-womens',
          'https://www.amazon.com/s?k=women+workout+sets&i=fashion-womens',
          'https://www.amazon.com/s?k=women+winter+jackets&i=fashion-womens',
        ],
        fashion_men: [
          'https://www.amazon.com/s?ref=nb_sb_noss_2&url=search-alias%3Dfashion-mens&field-keywords=',
          'https://www.amazon.com/gp/bestsellers/fashion/2475817011',
          'https://www.amazon.com/s?k=mens+shirts&i=fashion',
          'https://www.amazon.com/s?k=mens+shorts&i=fashion',
          'https://www.amazon.com/s?k=mens+joggers+sweatpants&i=fashion',
          'https://www.amazon.com/s?k=mens+hoodies&i=fashion',
          'https://www.amazon.com/s?k=mens+athletic+wear&i=fashion',
        ],
        accessories: [
          'https://www.amazon.com/gp/bestsellers/jewelry',
          'https://www.amazon.com/s?k=jewelry+women+sets&i=jewelry',
          'https://www.amazon.com/s?k=necklace+bracelet+earrings&i=jewelry',
          'https://www.amazon.com/s?k=handbags+purses&i=fashion',
          'https://www.amazon.com/s?k=watches+women&i=fashion',
          'https://www.amazon.com/s?k=sunglasses&i=fashion',
          'https://www.amazon.com/s?k=hair+accessories+clips&i=beauty',
        ],
        shoes: [
          'https://www.amazon.com/gp/bestsellers/shoes',
          'https://www.amazon.com/s?k=women+sneakers&i=shoes',
          'https://www.amazon.com/s?k=women+sandals&i=shoes',
          'https://www.amazon.com/s?k=women+ankle+boots&i=shoes',
          'https://www.amazon.com/s?k=men+running+shoes&i=shoes',
          'https://www.amazon.com/s?k=slippers&i=shoes',
        ],
        health_beauty: [
          'https://www.amazon.com/b?node=3760911',
          'https://www.amazon.com/gp/bestsellers/beauty',
          'https://www.amazon.com/s?k=skin+care+moisturizer&i=beauty',
          'https://www.amazon.com/s?k=hair+care&i=beauty',
          'https://www.amazon.com/s?k=makeup+cosmetics&i=beauty',
          'https://www.amazon.com/s?k=nail+care+gel+polish&i=beauty',
          'https://www.amazon.com/s?k=vitamins+supplements&i=hpc',
          'https://www.amazon.com/s?k=massage+tools&i=hpc',
          'https://www.amazon.com/s?k=hair+extensions+wigs&i=beauty',
        ],
        arts_crafts: [
          'https://www.amazon.com/b?node=2617941011',
          'https://www.amazon.com/gp/bestsellers/arts-and-crafts',
          'https://www.amazon.com/s?k=art+supplies+painting&i=arts-crafts',
          'https://www.amazon.com/s?k=sewing+supplies+fabric&i=arts-crafts',
          'https://www.amazon.com/s?k=craft+supplies+diy&i=arts-crafts',
        ],
        sports_fitness: [
          'https://www.amazon.com/b?node=3375251',
          'https://www.amazon.com/gp/bestsellers/sporting-goods',
          'https://www.amazon.com/s?k=yoga+mat&i=sporting',
          'https://www.amazon.com/s?k=resistance+bands&i=sporting',
          'https://www.amazon.com/s?k=gym+bags&i=sporting',
          'https://www.amazon.com/s?k=water+bottles+insulated&i=sporting',
          'https://www.amazon.com/s?k=outdoor+camping+gear&i=sporting',
          'https://www.amazon.com/s?k=hiking+backpack&i=sporting',
        ],
        appliances: [
          'https://www.amazon.com/b?node=2619525011',
          'https://www.amazon.com/gp/bestsellers/appliances',
          'https://www.amazon.com/s?k=air+fryer&i=appliances',
          'https://www.amazon.com/s?k=coffee+maker&i=appliances',
          'https://www.amazon.com/s?k=vacuum+cleaner&i=appliances',
          'https://www.amazon.com/s?k=blender&i=appliances',
        ],
        automotive: [
          'https://www.amazon.com/b?node=15684181',
          'https://www.amazon.com/gp/bestsellers/automotive',
          'https://www.amazon.com/s?k=car+seat+covers&i=automotive',
          'https://www.amazon.com/s?k=car+phone+mount&i=automotive',
          'https://www.amazon.com/s?k=car+organizer&i=automotive',
          'https://www.amazon.com/s?k=car+cleaning+supplies&i=automotive',
        ],
        pets: [
          'https://www.amazon.com/gp/bestsellers/pet-supplies',
          'https://www.amazon.com/s?k=dog+toys&i=pet-supplies',
          'https://www.amazon.com/s?k=cat+toys&i=pet-supplies',
          'https://www.amazon.com/s?k=pet+beds&i=pet-supplies',
          'https://www.amazon.com/s?k=dog+leash+collar&i=pet-supplies',
          'https://www.amazon.com/s?k=pet+grooming&i=pet-supplies',
        ],
        toys: [
          'https://www.amazon.com/gp/bestsellers/toys-and-games',
          'https://www.amazon.com/s?k=educational+toys+kids&i=toys-and-games',
          'https://www.amazon.com/s?k=board+games+family&i=toys-and-games',
          'https://www.amazon.com/s?k=outdoor+toys&i=toys-and-games',
          'https://www.amazon.com/s?k=remote+control+toys&i=toys-and-games',
        ],
        luggage_travel: [
          'https://www.amazon.com/gp/bestsellers/luggage',
          'https://www.amazon.com/s?k=travel+backpack&i=luggage',
          'https://www.amazon.com/s?k=packing+cubes&i=luggage',
          'https://www.amazon.com/s?k=crossbody+bag&i=fashion',
          'https://www.amazon.com/s?k=fanny+pack&i=fashion',
        ],
      };

      // Build flat list with dept tags for rotation tracking
      const AMAZON_PAGES = [];
      for (const [dept, urls] of Object.entries(AMAZON_PAGES_BY_DEPT)) {
        for (const url of urls) AMAZON_PAGES.push({ url, dept });
      }

      const productMap = {}; // asin → { asin, title, url }
      let pagesLoaded = 0;


      // Rotation: use excludeSet.size as offset so each "Fetch More" hits DIFFERENT pages
      // Target: 2 pages per dept on first call, alternate pages on subsequent calls
      const pageOffset = Math.floor(excludeSet.size / (count || 25));
      
      const pagesToFetch = [];
      const allDepts = [...new Set(AMAZON_PAGES.filter(p=>p.dept!=='deals').map(p=>p.dept))];
      const isSpecificDept = dept && dept !== 'all';

      // Only include general deals page when fetching all depts (not targeted fetch)
      if (!isSpecificDept) {
        const dealsPages = AMAZON_PAGES.filter(p => p.dept === 'deals');
        pagesToFetch.push(dealsPages[pageOffset % dealsPages.length]);
      }

      // For specific dept: fetch up to 8 pages from that dept only
      // For all depts: fetch 2 pages per dept with rotation
      const targetDepts = isSpecificDept ? [dept] : allDepts;
      for (const d of targetDepts) {
        const deptUrls = AMAZON_PAGES.filter(p => p.dept === d);
        if (!deptUrls.length) continue;
        const numPages = isSpecificDept ? Math.min(deptUrls.length, 8) : 2;
        for (let pi = 0; pi < numPages; pi++) {
          pagesToFetch.push(deptUrls[(pageOffset + pi) % deptUrls.length]);
        }
      }
      // Custom URLs override dept entirely
      if (customUrls && customUrls.length > 0) {
        pagesToFetch.length = 0;
        for (const cu of customUrls) pagesToFetch.push({ dept: 'custom', url: cu });
        console.log(`[dealsScrape] custom URLs mode — ${pagesToFetch.length} URLs`);
      } else {
        console.log(`[dealsScrape] dept="${dept||'all'}" → fetching ${pagesToFetch.length} pages: ${pagesToFetch.map(p=>p.dept).join(',').slice(0,80)}`);
      }

      const fetchPage_ = async ({ url, dept }) => {
        try {
          const h = await fetchPage(url, randUA());
          if (!h || h.length < 5000) return;
          let added = 0;
          // Pattern 1: "asin":"B0XXXXXXXX" — JSON blocks (most pages)
          for (const m of h.matchAll(/"asin"\s*:\s*"([B][0-9A-Z]{9})"/g))
            if (!productMap[m[1]]) { productMap[m[1]] = { asin: m[1], title: '', dept }; added++; }
          // Pattern 2: data-asin="B0XXXXXXXX" — search results, listing grids
          for (const m of h.matchAll(/data-asin="([B][0-9A-Z]{9})"/g))
            if (!productMap[m[1]]) { productMap[m[1]] = { asin: m[1], title: '', dept }; added++; }
          // Pattern 3: /slug/dp/B0XXXXXXXX — product links with title slug
          for (const m of h.matchAll(/href="[^"]*\/([^\/\s"]{5,})\/dp\/([B][0-9A-Z]{9})[^"]*"/g)) {
            const slug = decodeURIComponent(m[1]).replace(/-/g,' ');
            if (slug.length > 5 && !productMap[m[2]])
              productMap[m[2]] = { asin: m[2], title: slug, url: 'https://www.amazon.com/dp/'+m[2], dept };
          }
          // Pattern 4: /dp/B0XXXXXXXX with nearby alt= image
          for (const m of h.matchAll(/\/dp\/([B][0-9A-Z]{9})[^"]*"[^>]*>[\s\S]{0,300}?alt="([^"]{10,120})"/g)) {
            if (!productMap[m[1]] || !productMap[m[1]].title)
              productMap[m[1]] = { asin: m[1], title: m[2], url: 'https://www.amazon.com/dp/'+m[1], dept };
          }
          // Pattern 5: ANY /dp/B0XXXXXXXX in href (loosest — catches brand stores, carousels, lazy data)
          for (const m of h.matchAll(/\/dp\/([B][0-9A-Z]{9})/g))
            if (!productMap[m[1]]) { productMap[m[1]] = { asin: m[1], title: '', dept }; added++; }
          // Pattern 6: "productId":"B0XXXXXXXX" or "productAsin":"B0..." — React/JSON state blobs
          for (const m of h.matchAll(/"(?:productId|productAsin|childAsin|parentAsin|id|sku)"\s*:\s*"([B][0-9A-Z]{9})"/g))
            if (!productMap[m[1]]) { productMap[m[1]] = { asin: m[1], title: '', dept }; added++; }
          // Pattern 7: bare ASIN-looking strings inside Amazon's image URLs (m.media-amazon.com/.../B0XXXXXXXX...)
          for (const m of h.matchAll(/B0[0-9A-Z]{8}/g)) {
            const _a = m[0];
            if (!productMap[_a] && /B0[A-Z0-9]{8}/.test(_a)) {
              productMap[_a] = { asin: _a, title: '', dept }; added++;
            }
          }
          pagesLoaded++;
          console.log(`[dealsScrape] loaded ${dept} → ${url.slice(0,55)} → +${added} pool=${Object.keys(productMap).length}`);
        } catch(e) { console.error('[dealsScrape] page error:', e.message); }
      };

      // Fetch in parallel batches of 5 (more pages = more variety)
      console.log(`[dealsScrape] fetching ${pagesToFetch.length} pages across ${allDepts.length+1} depts`);
      for (let i = 0; i < pagesToFetch.length; i += 5) {
        await Promise.all(pagesToFetch.slice(i, i+5).map(fetchPage_));
        if (Object.keys(productMap).length >= excludeSet.size + count + 150) break;
        if (i+5 < pagesToFetch.length) await new Promise(r => setTimeout(r, 400));
      }

      if (Object.keys(productMap).length === 0)
        return res.json({ success: false, error: 'Could not load Amazon pages. Try again in a moment.' });

      console.log('[dealsScrape] total pool after', pagesLoaded, 'pages:', Object.keys(productMap).length);
      // Filter: exclude already-used, VeRO, and digital items
      const allProducts = Object.values(productMap).filter(p =>
        p.asin &&
        !excludeSet.has(p.asin) &&
        !isVero(p.title, '') &&
        // When specific dept requested, only include products from that dept
        (!isSpecificDept || p.dept === dept || p.dept === 'deals' && !isSpecificDept)
      );

      // Dept-balanced selection: take products proportionally from each dept
      // This ensures inventory has variety (not 20 kitchen gadgets and 5 everything else)
      const byDept = {};
      for (const p of allProducts) {
        const d = p.dept || 'unknown';
        (byDept[d] = byDept[d] || []).push(p);
      }
      // Shuffle within each dept
      for (const arr of Object.values(byDept)) {
        for (let i = arr.length-1; i>0; i--) {
          const j = Math.floor(Math.random()*(i+1));
          [arr[i],arr[j]]=[arr[j],arr[i]];
        }
      }
      // Round-robin across depts to get diverse selection
      const deptQueues = Object.values(byDept);
      const selected = [];
      let _di = 0;
      while (selected.length < count && deptQueues.some(q=>q.length>0)) {
        const q = deptQueues[_di % deptQueues.length];
        if (q.length > 0) selected.push(q.shift());
        _di++;
      }
      console.log(`[dealsScrape] total=${Object.keys(productMap).length} after filter=${allProducts.length} selected=${selected.length}`);
      return res.json({ success: true, products: selected, totalFound: Object.keys(productMap).length });
    }


    // ── AliExpress: fetch product list from category pages ─────────────────────
    if (action === 'aeScrapeDeals') {
      const { dept = null, exclude = [], count = 25, maxDeliveryDays = 21, minRating = 4.5, minOrders = 50 } = body;
      const excludeSet = new Set(exclude);

      const deptUrls = dept && AE_DEPT_URLS[dept] ? AE_DEPT_URLS[dept] : null;
      const normalizedPages = deptUrls
        ? deptUrls.map(url => ({ url, dept: dept }))
        : Object.entries(AE_DEPT_URLS).flatMap(([d, urls]) => urls.slice(0, 2).map(url => ({ url, dept: d })));

      const productMap = {};
      const aeHeaders = {
        'User-Agent': randUA(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cookie': 'aep_usuc_f=site=glo&c_tp=USD&x_alilg=en&b_locale=en_US',
        'Cache-Control': 'no-cache',
      };

      for (let i = 0; i < normalizedPages.length; i += 4) {
        await Promise.all(normalizedPages.slice(i, i+4).map(async ({ url, dept: d }) => {
          try {
            const r = await fetch(url, { headers: aeHeaders, redirect: 'follow' });
            const html = await r.text();
            if (!html || html.length < 5000) return;
            const items = aeExtractListings(html);
            let added = 0;
            for (const item of items) {
              if (!item.aeId || excludeSet.has(item.aeId)) continue;
              if (!productMap[item.aeId]) { productMap[item.aeId] = { ...item, _aeDept: d }; added++; }
            }
            console.log(`[aeScrapeDeals] ${d} -> +${added} pool=${Object.keys(productMap).length}`);
          } catch(e) { console.error('[aeScrapeDeals] page error:', e.message); }
        }));
        if (Object.keys(productMap).length >= count + 100) break;
        if (i + 4 < normalizedPages.length) await sleep(600);
      }

      const filtered = Object.values(productMap).filter(p =>
        !excludeSet.has(p.aeId) && p.salePrice > 0 &&
        p.ordersNum >= (minOrders || 0) // rating/delivery filtered by shipFromCountry=US in URL
      );
      for (let i = filtered.length-1; i>0; i--) { const j = Math.floor(Math.random()*(i+1)); [filtered[i],filtered[j]]=[filtered[j],filtered[i]]; }
      const selected = filtered.slice(0, count);
      console.log(`[aeScrapeDeals] pool=${Object.keys(productMap).length} filtered=${filtered.length} selected=${selected.length}`);
      return res.json({ success: true, products: selected, totalFound: Object.keys(productMap).length });
    }

    // ── AliExpress: scrape/build product from hint data or clientHtml ────────────
    // For bulk import: list page data (hintData) is sufficient for a basic listing
    // For individual import: browser sends clientHtml of the product page
    if (action === 'aeScrapeProduct') {
      const { aeId, hintTitle, hintPrice, hintImg, hintOrders, hintDept, clientHtml } = body;
      if (!aeId) return res.status(400).json({ success: false, error: 'aeId required' });

      // If browser sent the product page HTML, try to parse full details
      if (clientHtml && clientHtml.length > 10000) {
        const parsed = aeScrapeProduct(clientHtml, aeId);
        if (parsed && parsed.price > 0) {
          console.log(`[aeScrapeProduct] ${aeId} parsed from clientHtml: "${parsed.title?.slice(0,40)}" $${parsed.price} vars=${parsed.variations?.length||0}`);
          return res.json({ success: true, product: parsed });
        }
      }

      // ── Server fetch for full details (variations, combos, etc.) ────────────
      // For bulk import we have hintTitle/hintPrice from the list page card, but
      // hint data alone has no variation info. Fetch the actual product page
      // server-side so the AE scrapers can extract dimensions. If the fetch
      // fails (Amazon-style bot block, redirect to login, etc.), fall through
      // to the hint-only branch below.
      const _aeUrlsBulk = [
        `https://www.aliexpress.com/item/${aeId}.html`,
        `https://www.aliexpress.com/i/${aeId}.html`,
        `https://www.aliexpress.us/item/${aeId}.html`,
      ];
      const _aeHdrsBulk = {
        'User-Agent': randUA(),
        'Accept': 'text/html,application/xhtml+xml,*/*;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cookie': 'aep_usuc_f=site=glo&c_tp=USD&x_alilg=en&b_locale=en_US; intl_locale=en_US; acs_usuc_f=;',
        'Referer': 'https://www.google.com/',
      };
      let _aeHtmlBulk = null;
      for (const _aeUrl of _aeUrlsBulk) {
        if (_aeHtmlBulk) break;
        for (let a = 0; a < 2 && !_aeHtmlBulk; a++) {
          try {
            const r = await fetch(_aeUrl, {
              headers: { ..._aeHdrsBulk, 'User-Agent': UA_LIST[a % UA_LIST.length] },
              redirect: 'follow',
              timeout: 15000,
            });
            const h = await r.text();
            if (!h || h.length < 5000) continue;
            if (h.includes('window.runParams') && h.includes('skuModule')) { _aeHtmlBulk = h; break; }
            if (h.includes('sku-item--property') || h.includes('sku-item--skus')) { _aeHtmlBulk = h; break; }
          } catch(e) { console.warn(`[aeScrapeProduct bulk-fetch] ${aeId} attempt ${a}:`, e.message); }
        }
      }
      if (_aeHtmlBulk) {
        const parsed = aeScrapeProduct(_aeHtmlBulk, aeId);
        if (parsed?.price > 0) {
          // Merge hint data as fallback for any field the parser missed
          if (!parsed.images?.length && hintImg) {
            parsed.images = [hintImg.startsWith('//') ? 'https:' + hintImg : hintImg];
          }
          if (!parsed.title && hintTitle) parsed.title = hintTitle.trim();
          console.log(`[aeScrapeProduct] ${aeId} bulk-fetched: "${parsed.title?.slice(0,40)}" $${parsed.price} vars=${parsed.variations?.length||0} combos=${Object.keys(parsed.comboPrices||{}).length}`);
          return res.json({ success: true, product: parsed });
        }
      }

      // For bulk import: fall back to hint data only when product page fetch fails
      // (rare — usually means AE blocked the IP or the item was removed)
      if (hintTitle && hintPrice > 0) {
        const title = hintTitle.trim();
        const ebayTitle = title.replace(/[^a-zA-Z0-9 ,&\-().'/]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 80);
        const img = hintImg?.startsWith('//') ? 'https:' + hintImg : (hintImg || '');
        const product = {
          aeId, title, ebayTitle,
          price: parseFloat(hintPrice) || 0,
          aeShipping: 0, // US warehouse = free shipping
          images: img ? [img] : [],
          description: `${title}. Ships from US warehouse.`,
          aspects: {},
          hasVariations: false, variations: [], comboPrices: {},
          inStock: true,
          rating: 0,
        };
        console.log(`[aeScrapeProduct] ${aeId} hint-only fallback (server fetch failed): "${title.slice(0,40)}" $${product.price}`);
        return res.json({ success: true, product });
      }

      // (server fetch is now performed above, before the hint fallback)
      console.warn(`[aeScrapeProduct] ${aeId} — no clientHtml, no hint, no server html`);
      return res.json({ success: false, error: 'Could not fetch product details — missing hint data' });
    }

    // ── LINK OFFERS: discover offer IDs for all variants of a listing ──────────
    if (action === 'linkOffers') {
      const { access_token, ebaySku, ebayListingId, skuToAsin } = body;
      if (!access_token || !ebaySku) return res.status(400).json({ error: 'Missing access_token or ebaySku' });

      const EBAY_API = getEbayUrls().EBAY_API;
      const normSku  = ebaySku.trim().replace(/\s+/g, '');
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };
      console.log(`[fixOffers] ${normSku}`);

      // Collect candidate SKUs from all sources — fastest first
      const candidateSkus = new Set();

      // 1. From skuToAsin sent by frontend (most reliable — built at push time)
      for (const sku of Object.keys(skuToAsin || {})) candidateSkus.add(sku);

      // 2. From inventory_item_group variantSKUs
      const grpR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(normSku)}`, { headers: auth }).catch(() => null);
      if (grpR?.ok) {
        const gd = await grpR.json().catch(() => ({}));
        for (const sku of (gd.variantSKUs || [])) candidateSkus.add(sku);
      }

      // 3. Scan all offers filtering by listingId (finds PUBLISHED offers fast)
      const _lid = String(ebayListingId || '');
      if (_lid) {
        let _off = 0;
        while (_off < 1000) {
          const _or = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=100&offset=${_off}`, { headers: auth }).catch(() => null);
          if (!_or?.ok) break;
          const _od = await _or.json().catch(() => ({}));
          for (const o of (_od.offers || [])) {
            if (o.sku && o.listing?.listingId === _lid) candidateSkus.add(o.sku);
          }
          if ((_od.offers||[]).length < 100) break;
          _off += 100;
          await sleep(80);
        }
      }

      // 4. If still few candidates, do a limited inventory item scan (max 500 items)
      if (candidateSkus.size < 3) {
        const normSkuUpper = normSku.toUpperCase();
        let _off = 0;
        while (_off < 500) {
          const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item?limit=100&offset=${_off}`, { headers: auth }).catch(() => null);
          if (!ir?.ok) break;
          const id = await ir.json().catch(() => ({}));
          for (const item of (id.inventoryItems || []))
            if (item.sku?.toUpperCase().startsWith(normSkuUpper + '-')) candidateSkus.add(item.sku);
          if ((id.inventoryItems||[]).length < 100) break;
          _off += 100;
          await sleep(80);
        }
      }

      console.log(`[fixOffers] ${candidateSkus.size} candidate SKUs`);
      if (!candidateSkus.size) return res.json({ success: false, error: 'No variant SKUs found' });

      // GET offer for each candidate SKU in parallel batches
      const offerMap = {};
      const skuList = [...candidateSkus];
      for (let i = 0; i < skuList.length; i += 20) {
        await Promise.all(skuList.slice(i, i + 20).map(async sku => {
          const r = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}&limit=1`, { headers: auth }).catch(() => null);
          if (r?.ok) {
            const d = await r.json().catch(() => ({}));
            const o = (d.offers || [])[0];
            if (o?.offerId) offerMap[sku] = o.offerId;
          }
        }));
        if (i + 20 < skuList.length) await sleep(60);
      }
      console.log(`[fixOffers] found ${Object.keys(offerMap).length}/${candidateSkus.size} offer IDs`);

      return res.json({
        success: true,
        skusFound: candidateSkus.size,
        offersFound: Object.keys(offerMap).length,
        offerIds: offerMap,
        skus: skuList,
      });
    }

    if (action === 'endListing') {
      const { access_token, ebaySku, ebayListingId } = body;
      const EBAY_API = getEbayUrls().EBAY_API; // production only
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };
      let ended = 0;
      const variantSkus = [];

      // Strategy A: get group to find all variant SKUs
      const groupRes = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
      if (groupRes.variantSKUs?.length) variantSkus.push(...groupRes.variantSKUs);
      console.log(`[end] group=${ebaySku} variantSkus=${variantSkus.length}`);

      // Strategy B: if group returned no SKUs, page through all offers and find ones
      // belonging to this listing via ebayListingId
      if (!variantSkus.length) {
        console.log('[end] group empty — scanning offers by listing ID:', ebayListingId);
        let offset = 0;
        while (true) {
          const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=100&offset=${offset}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
          const offers = ol.offers || [];
          if (!offers.length) break;
          for (const o of offers) {
            // Match by listing ID or by SKU prefix
            const matchesListing = ebayListingId && o.listing?.listingId === String(ebayListingId);
            const matchesSku = o.sku?.startsWith(ebaySku.split('-').slice(0,3).join('-'));
            if (matchesListing || matchesSku) {
              if (!variantSkus.includes(o.sku)) variantSkus.push(o.sku);
            }
          }
          if (offers.length < 100) break;
          offset += 100;
        }
        console.log(`[end] scan found ${variantSkus.length} variant skus`);
      }

      // Collect all offer IDs across all variant SKUs
      const allSkus = variantSkus.length ? variantSkus : [ebaySku];
      const allOfferIds = new Set();
      let publishedOfferId = null; // one published offer = end the whole listing

      for (const sku of allSkus) {
        const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
        for (const offer of (ol.offers||[])) {
          allOfferIds.add(offer.offerId);
          if (offer.status === 'PUBLISHED' && !publishedOfferId) publishedOfferId = offer.offerId;
        }
      }

      // For multi-variation listings: withdrawing ONE published offer ends the whole listing
      // Then delete all individual offers and items
      if (publishedOfferId) {
        const wr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${publishedOfferId}/withdraw`, { method: 'POST', headers: auth });
        const wb = await wr.json().catch(()=>({}));
        console.log(`[end] withdraw offer ${publishedOfferId}: ${wr.status} ${JSON.stringify(wb).slice(0,80)}`);
        if (wr.status < 300) ended++;
      } else if (ebayListingId) {
        // Fallback: try withdrawing by listing ID via trading API
        console.log(`[end] no published offer found — trying bulk withdraw via offer scan`);
        let offset2 = 0;
        while (allOfferIds.size < 600) {
          const ol2 = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=100&offset=${offset2}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
          const offs2 = ol2.offers || [];
          if (!offs2.length) break;
          for (const o2 of offs2) {
            if (o2.listing?.listingId === String(ebayListingId)) {
              allOfferIds.add(o2.offerId);
              if (o2.status === 'PUBLISHED' && !publishedOfferId) {
                publishedOfferId = o2.offerId;
                const wr2 = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${o2.offerId}/withdraw`, { method: 'POST', headers: auth });
                if (wr2.status < 300) ended++;
                console.log(`[end] fallback withdraw ${o2.offerId}: ${wr2.status}`);
              }
            }
          }
          if (offs2.length < 100) break;
          offset2 += 100;
        }
      }

      // Delete all collected offers
      for (const offerId of allOfferIds) {
        await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerId}`, { method: 'DELETE', headers: auth }).catch(()=>{});
      }

      // Delete inventory item group
      await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, { method: 'DELETE', headers: auth }).catch(()=>{});

      // Delete all variant inventory items in batches of 25
      for (let i = 0; i < variantSkus.length; i += 25) {
        const batch = variantSkus.slice(i, i+25);
        await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item?sku=${batch.map(encodeURIComponent).join(',')}`, { method: 'DELETE', headers: auth }).catch(()=>{});
      }

      console.log(`[end] done ended=${ended} deletedItems=${variantSkus.length}`);
      return res.json({ success: true, ended, deleted: variantSkus.length });
    }

    if (action === 'getOrders') {
      const { access_token, limit = 50 } = body;
      const fromDate = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString();
      const r = await fetch(
        `https://api.ebay.com/sell/fulfillment/v1/order?limit=${limit}&filter=lastmodifieddate:[${fromDate}..]`,
        { headers: { 'Authorization': `Bearer ${access_token}`, 'Content-Type': 'application/json' } }
      );
      const d = await r.json();
      if (!r.ok) return res.status(r.status).json({ error: d.errors?.[0]?.message || JSON.stringify(d) });
      return res.json({ orders: d.orders || [], total: d.total || 0 });
    }

    // ── AUTO-REPLENISH — set qty=1 for specific sold SKU variants (no scrape needed) ──
    // Called when an order is detected for a tracked listing.
    // Just bumps the sold variant's inventory quantity back to 1.
    if (action === 'autoReplenish') {
      const { access_token, skus } = body; // skus = [{sku, variantSku}] or just [{sku}]
      if (!access_token || !skus?.length) return res.status(400).json({ error: 'Missing access_token or skus' });
      const EBAY_API = getEbayUrls().EBAY_API;
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Content-Language': 'en-US', 'Accept-Language': 'en-US', 'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US' };
      const results = [];

      // For each SKU: fetch current inventory item, set qty=1, PUT back
      for (const { sku, variantSku } of skus) {
        const targetSku = variantSku || sku; // variantSku is the specific variation sold
        if (!targetSku) continue;
        try {
          // Get current item to preserve aspects/product info
          const getR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(targetSku)}`,
            { headers: auth });
          if (!getR.ok) {
            console.warn(`[autoReplenish] can't fetch ${targetSku}: ${getR.status}`);
            results.push({ sku: targetSku, ok: false, error: `fetch ${getR.status}` });
            continue;
          }
          const item = await getR.json();

          // Set qty to 1
          item.availability = item.availability || {};
          item.availability.shipToLocationAvailability = { quantity: 1 };
          // Remove read-only fields
          delete item.locale; // will be set in PUT

          const putR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(targetSku)}`,
            { method: 'PUT', headers: auth, body: JSON.stringify(item) });
          if (putR.ok || putR.status === 204) {
            console.log(`[autoReplenish] ${targetSku}: qty set to 1`);
            results.push({ sku: targetSku, ok: true });
          } else {
            const errTxt = await putR.text().catch(() => '');
            console.warn(`[autoReplenish] ${targetSku} PUT failed: ${putR.status} ${errTxt.slice(0,80)}`);
            results.push({ sku: targetSku, ok: false, error: `PUT ${putR.status}` });
          }
          await sleep(200);
        } catch(e) {
          results.push({ sku: targetSku, ok: false, error: e.message });
        }
      }

      const ok = results.filter(r => r.ok).length;
      console.log(`[autoReplenish] ${ok}/${results.length} SKUs replenished to qty=1`);
      return res.json({ success: ok > 0, replenished: ok, total: results.length, results });
    }

    // ── RECOVER SKUS — maps all eBay listing IDs → real SKUs ────────────────
    if (action === 'recoverSkus') {
      const { access_token } = body;
      if (!access_token) return res.status(400).json({ error: 'Missing access_token' });
      const EBAY_API = getEbayUrls().EBAY_API;
      const auth = { Authorization: `Bearer ${access_token}`, 'Accept-Language': 'en-US' };
      const skuMap = {}; // listingId → sku
      let offset = 0, hasMore = true;
      while (hasMore && offset < 2000) {
        const r = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=100&offset=${offset}`, { headers: auth });
        const d = await r.json();
        const offers = d.offers || [];
        for (const o of offers) {
          const lid = o.listing?.listingId;
          if (lid && o.sku) skuMap[lid] = o.sku;
        }
        hasMore = offers.length === 100;
        offset += 100;
        if (offers.length) await new Promise(r => setTimeout(r, 100));
      }
      return res.json({ skuMap, total: Object.keys(skuMap).length });
    }

    // ── DEBUG INVENTORY: fetch live eBay inventory state for revise preview ────────
    if (action === 'debugInventory') {
      const { access_token, ebaySku, ebayListingId } = body;
      if (!access_token || !ebaySku) return res.status(400).json({ error: 'Missing access_token or ebaySku' });
      const EBAY_API = getEbayUrls().EBAY_API;
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };
      try {
        // Get group variant SKUs
        const grpR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, { headers: auth }).catch(()=>null);
        const grpD = grpR?.ok ? await grpR.json().catch(()=>({})) : {};
        const skus = grpD.variantSKUs?.length ? grpD.variantSKUs : [ebaySku];

        // Fetch up to 25 sample variants for preview
        const samples = [];
        const sampleSkus = skus.slice(0, 25);
        await Promise.all(sampleSkus.map(async sku => {
          try {
            const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, { headers: auth });
            const id = ir.ok ? await ir.json().catch(()=>({})) : {};
            const ofR = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}`, { headers: auth });
            const ofD = ofR.ok ? await ofR.json().catch(()=>({})) : {};
            const offer = (ofD.offers||[])[0] || {};
            samples.push({
              sku: sku.slice(-30),
              qty: id.availability?.shipToLocationAvailability?.quantity ?? '?',
              price: offer.pricingSummary?.price?.value ?? null,
              status: offer.status ?? '?',
              offerId: offer.offerId ?? null,
            });
          } catch(e) {}
        }));
        samples.sort((a,b) => (b.qty||0) - (a.qty||0));
        return res.json({ success: true, variantCount: skus.length, variantSamples: samples, groupSku: ebaySku, listingId: ebayListingId || '' });
      } catch(e) { return res.status(500).json({ error: e.message }); }
    }

    // ── GET TRADING ITEM: fetch actual live prices via Trading API GetItem ───────
    if (action === 'getTradingItem') {
      const { access_token, itemId } = body;
      if (!access_token || !itemId) return res.status(400).json({ error: 'Missing access_token or itemId' });
      const { EBAY_TRADING, CLIENT_ID } = getEbayUrls();
      try {
        const xml = `<?xml version="1.0" encoding="utf-8"?>
<GetItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <RequesterCredentials><eBayAuthToken>${access_token}</eBayAuthToken></RequesterCredentials>
  <ItemID>${itemId}</ItemID>
  <DetailLevel>ReturnAll</DetailLevel>
  <IncludeVariations>true</IncludeVariations>
</GetItemRequest>`;
        const r = await fetch(EBAY_TRADING, {
          method: 'POST',
          headers: {
            'Content-Type': 'text/xml',
            'X-EBAY-API-SITEID': '0',
            'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
            'X-EBAY-API-CALL-NAME': 'GetItem',
            'X-EBAY-API-APP-NAME': CLIENT_ID || '',
          },
          body: xml,
        });
        const text = await r.text();
        // Parse variation prices from XML
        const variations = [];
        const varMatches = [...text.matchAll(/<Variation>[\s\S]*?<\/Variation>/g)];
        for (const vm of varMatches) {
          const sku   = vm[0].match(/<SKU>(.*?)<\/SKU>/)?.[1] || '';
          const price = vm[0].match(/<StartPrice[^>]*>(.*?)<\/StartPrice>/)?.[1] || '';
          const qty   = vm[0].match(/<Quantity>(\d+)<\/Quantity>/)?.[1] || '0';
          const sold  = vm[0].match(/<QuantitySold>(\d+)<\/QuantitySold>/)?.[1] || '0';
          if (sku) variations.push({ sku, price: parseFloat(price)||0, qty: parseInt(qty)||0, sold: parseInt(sold)||0 });
        }
        const itemTitle = text.match(/<Title>(.*?)<\/Title>/)?.[1] || '';
        const listingStatus = text.match(/<ListingStatus>(.*?)<\/ListingStatus>/)?.[1] || '';
        return res.json({ success: true, itemId, itemTitle, listingStatus, variations, raw: text.length });
      } catch(e) { return res.status(500).json({ error: e.message }); }
    }

    // ── MARK OOS: set qty=0 for all variants of a listing instantly ─────────────
    // Emergency stop — use when pricing is wrong and you need to stop orders immediately.
    // No scraping. Just reads variantSKUs from group and bulk-sets all to qty=0.
    if (action === 'markOos') {
      // Zero out all offer quantities via inventory items — stops orders immediately without ending listing.
      // Uses PUT inventory_item (qty=0) which is the correct way to zero stock,
      // then bulk_update_price_quantity to sync offer quantities.
      const { access_token, ebaySku, ebayListingId } = body;
      if (!access_token || !ebaySku) return res.status(400).json({ error: 'Missing access_token or ebaySku' });
      const EBAY_API = getEbayUrls().EBAY_API;
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Content-Language': 'en-US', 'Accept-Language': 'en-US' };

      // Strategy 1: Get variant SKUs from group
      let skus = [];
      const grpR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, { headers: auth }).catch(()=>null);
      if (grpR?.ok) {
        const grpD = await grpR.json().catch(()=>({}));
        skus = grpD.variantSKUs || [];
      }

      // Strategy 2: If group empty, scan offers by listingId to find all variant SKUs
      if (!skus.length && ebayListingId) {
        const _skuPrefix = ebaySku.split('-').slice(0, 3).join('-');
        let offset = 0;
        while (skus.length < 500) {
          const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=100&offset=${offset}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
          const offers = ol.offers || [];
          if (!offers.length) break;
          for (const o of offers) {
            const matchesListing = o.listing?.listingId === String(ebayListingId);
            const matchesSku = o.sku?.startsWith(_skuPrefix);
            if ((matchesListing || matchesSku) && !skus.includes(o.sku)) skus.push(o.sku);
          }
          if (offers.length < 100) break;
          offset += 100;
        }
      }

      // Fallback: treat the group SKU as the only item
      if (!skus.length) skus = [ebaySku];
      console.log(`[markOos] zeroing qty for ${skus.length} SKUs of ${ebaySku}`);

      // Step 1: PUT each inventory item with availableQuantity=0
      // This sets the underlying stock to 0 so eBay hides the listing from search
      let itemsZeroed = 0;
      for (let i = 0; i < skus.length; i += 15) {
        await Promise.all(skus.slice(i, i + 15).map(async sku => {
          try {
            // GET current item to preserve all its fields
            const getR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, { headers: auth });
            if (!getR.ok) return;
            const item = await getR.json().catch(()=>({}));
            // Zero out the quantity only
            if (item.availability) {
              item.availability.shipToLocationAvailability = { quantity: 0 };
            } else {
              item.availability = { shipToLocationAvailability: { quantity: 0 } };
            }
            const putR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`,
              { method: 'PUT', headers: auth, body: JSON.stringify(item) });
            if (putR.ok || putR.status === 204) itemsZeroed++;
          } catch(e) {}
        }));
        if (i + 15 < skus.length) await sleep(80);
      }

      // Step 2: Collect offer IDs and bulk-zero their quantities too
      const offerIds = [];
      for (let i = 0; i < skus.length; i += 20) {
        await Promise.all(skus.slice(i, i + 20).map(async sku => {
          const r = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}`, { headers: auth }).catch(()=>null);
          const d = await r?.json().catch(()=>({}));
          for (const o of (d?.offers||[])) {
            if (o.offerId && !offerIds.includes(o.offerId)) offerIds.push(o.offerId);
          }
        }));
        if (i + 20 < skus.length) await sleep(80);
      }

      let ok = 0, fail = 0;
      for (let _bi = 0; _bi < offerIds.length; _bi += 25) {
        const batch = offerIds.slice(_bi, _bi + 25).map(offerId => ({
          offerId, availableQuantity: 0,
        }));
        const br = await fetch(`${EBAY_API}/sell/inventory/v1/bulk_update_price_quantity`, {
          method: 'POST', headers: auth, body: JSON.stringify({ requests: batch }),
        }).catch(()=>null);
        if (br?.ok) {
          const bd = await br.json().catch(()=>({}));
          for (const resp of (bd.responses||[])) {
            if ((resp.statusCode >= 200 && resp.statusCode < 300) || resp.offerId) ok++;
            else fail++;
          }
        } else fail += batch.length;
        if (_bi + 25 < offerIds.length) await sleep(80);
      }

      console.log(`[markOos] done: ${skus.length} SKUs, ${itemsZeroed} items zeroed, ${ok} offers zeroed, ${fail} failed`);
      return res.json({ success: (ok > 0 || itemsZeroed > 0 || offerIds.length === 0), markedOos: ok || itemsZeroed, zeroed: ok, itemsZeroed, failed: fail, totalOffers: offerIds.length, totalVariants: skus.length });
    }

    return res.status(400).json({ error: `Unknown action: ${action}` });

  } catch (err) {
    console.error('[Error]', err.message);
    return res.status(500).json({ error: err.message });
  }
};

// ── In-process invocation helper ─────────────────────────────────────────────
// Lets worker.js (and internal self-calls) invoke any action without HTTP.
// Mocks Express req/res so the existing handler body runs unchanged.
// Returns { ok, status, data } — same shape regardless of how the handler responded.
module.exports.callAction = async function callEbayAction(body) {
  const mockReq = { method: 'POST', query: {}, body: body || {}, headers: {} };
  let _status = 200;
  let _data = null;
  const mockRes = {
    setHeader() { return this; },
    status(code) { _status = code; return this; },
    json(obj)    { if (_data === null) _data = obj; return this; },
    send(obj)    { if (_data === null) _data = obj; return this; },
    end()        { return this; },
  };
  try {
    await module.exports(mockReq, mockRes);
  } catch (e) {
    return { ok: false, status: 500, data: { error: e.message } };
  }
  return { ok: _status >= 200 && _status < 300, status: _status, data: _data };
};

// Also expose scrapeAmazonProduct directly so the sync action can call it in-process
module.exports.scrapeAmazonProduct = scrapeAmazonProduct;

// ══════════════════════════════════════════════════════════════════════
// ALIEXPRESS SCRAPER — Completely separate from Amazon scraper
// Uses window.runParams JSON embedded in AliExpress HTML
// ══════════════════════════════════════════════════════════════════════

// AliExpress department → category page URLs
// All use shipfromcountry=US to pre-filter US warehouse items (fast delivery, no tariff issues)
// URL format: /w/wholesale-keyword.html?shipfromcountry=US&sorttype=default&page=N
const AE_BASE = 'https://www.aliexpress.com/w/wholesale-';
const AE_PARAMS = '.html?shipfromcountry=US&sorttype=default&page=1';
const AE_PARAMS2 = '.html?shipfromcountry=US&sorttype=default&page=2';

const AE_DEPT_URLS = {
  women:       [AE_BASE+'women-dress'+AE_PARAMS, AE_BASE+'women-tops'+AE_PARAMS, AE_BASE+'women-leggings'+AE_PARAMS, AE_BASE+'women-blouse'+AE_PARAMS2, AE_BASE+'women-dress'+AE_PARAMS2],
  men:         [AE_BASE+'men-shirts'+AE_PARAMS, AE_BASE+'men-hoodies'+AE_PARAMS, AE_BASE+'men-shorts'+AE_PARAMS, AE_BASE+'men-joggers'+AE_PARAMS],
  kids:        [AE_BASE+'girls-clothing'+AE_PARAMS, AE_BASE+'boys-clothing'+AE_PARAMS, AE_BASE+'kids-dresses'+AE_PARAMS, AE_BASE+'kids-sets'+AE_PARAMS],
  shoes:       [AE_BASE+'women-sneakers'+AE_PARAMS, AE_BASE+'women-sandals'+AE_PARAMS, AE_BASE+'men-sneakers'+AE_PARAMS, AE_BASE+'women-boots'+AE_PARAMS],
  bags:        [AE_BASE+'women-handbags'+AE_PARAMS, AE_BASE+'crossbody-bags'+AE_PARAMS, AE_BASE+'women-jewelry'+AE_PARAMS, AE_BASE+'tote-bags'+AE_PARAMS],
  home:        [AE_BASE+'home-decor'+AE_PARAMS, AE_BASE+'throw-pillows'+AE_PARAMS, AE_BASE+'area-rugs'+AE_PARAMS, AE_BASE+'curtains'+AE_PARAMS, AE_BASE+'wall-art'+AE_PARAMS],
  kitchen:     [AE_BASE+'kitchen-gadgets'+AE_PARAMS, AE_BASE+'kitchen-organizer'+AE_PARAMS, AE_BASE+'cooking-utensils'+AE_PARAMS, AE_BASE+'food-storage'+AE_PARAMS],
  beauty:      [AE_BASE+'skin-care'+AE_PARAMS, AE_BASE+'hair-care'+AE_PARAMS, AE_BASE+'makeup-brushes'+AE_PARAMS, AE_BASE+'nail-care'+AE_PARAMS],
  sports:      [AE_BASE+'yoga-mat'+AE_PARAMS, AE_BASE+'resistance-bands'+AE_PARAMS, AE_BASE+'gym-bag'+AE_PARAMS, AE_BASE+'water-bottle'+AE_PARAMS],
  pets:        [AE_BASE+'dog-toys'+AE_PARAMS, AE_BASE+'cat-toys'+AE_PARAMS, AE_BASE+'pet-bed'+AE_PARAMS, AE_BASE+'dog-collar'+AE_PARAMS],
  toys:        [AE_BASE+'kids-toys'+AE_PARAMS, AE_BASE+'educational-toys'+AE_PARAMS, AE_BASE+'dolls'+AE_PARAMS, AE_BASE+'board-games'+AE_PARAMS],
  electronics: [AE_BASE+'phone-case'+AE_PARAMS, AE_BASE+'wireless-earbuds'+AE_PARAMS, AE_BASE+'power-bank'+AE_PARAMS, AE_BASE+'smart-watch'+AE_PARAMS],
  tools:       [AE_BASE+'tool-set'+AE_PARAMS, AE_BASE+'led-strip-lights'+AE_PARAMS, AE_BASE+'storage-organizer'+AE_PARAMS, AE_BASE+'smart-home'+AE_PARAMS],
  automotive:  [AE_BASE+'car-seat-cover'+AE_PARAMS, AE_BASE+'car-phone-holder'+AE_PARAMS, AE_BASE+'car-organizer'+AE_PARAMS, AE_BASE+'car-accessories'+AE_PARAMS],
};

// Extract AliExpress product listings from a category/search page HTML
// Uses bracket-counting parser on the "itemList":{"content":[...]} JSON blob
function aeExtractListings(html) {
  if (!html || html.length < 1000) return [];
  const products = [];

  // Find the itemList content array using string marker + bracket counting
  // (regex approach fails on large HTML due to backtracking limits)
  const marker = '"itemList":{"content":';
  const markerIdx = html.indexOf(marker);
  if (markerIdx < 0) return products;

  const arrStart = markerIdx + marker.length;
  let depth = 0, i = arrStart, end = -1;
  for (; i < html.length && i < arrStart + 600000; i++) {
    const c = html[i];
    if (c === '[' || c === '{') depth++;
    else if (c === ']' || c === '}') { depth--; if (depth === 0) { end = i; break; } }
  }
  if (end < 0) return products;

  let items = [];
  try { items = JSON.parse(html.slice(arrStart, end + 1)); } catch(e) {
    console.warn('[aeExtract] JSON parse error:', e.message);
    return products;
  }

  for (const item of items) {
    const productId = item.productId || item.redirectedId || item.itemId;
    if (!productId) continue;
    const title = item.title?.displayTitle || item.name || '';
    if (!title) continue;
    const salePrice = item.prices?.salePrice?.minPrice || item.prices?.activityPrice?.minPrice || 0;
    const img = item.image?.imgUrl || item.images?.[0] || '';
    const imgUrl = img.startsWith('//') ? 'https:' + img : (img || '');
    const tradeDesc = item.trade?.tradeDesc || item.tradeDesc || '';
    const ordersNum = parseInt(tradeDesc.replace(/[^0-9]/g,'')) || 0;

    products.push({
      aeId: String(productId),
      title,
      salePrice: parseFloat(salePrice) || 0,
      imgUrl,
      ordersNum,
    });
  }
  console.log(`[aeExtract] extracted ${products.length} products from ${items.length} items`);
  return products;
}

// Scrape AliExpress product from rendered HTML (aliexpress.us React DOM)
// Two strategies: (1) window.runParams JSON on aliexpress.com, (2) DOM parsing on aliexpress.us
function aeScrapeProduct(html, aeId) {
  if (!html || html.length < 1000) return null;

  // Strategy 1: window.runParams (aliexpress.com server-rendered pages)
  const rpIdx = html.indexOf('window.runParams');
  if (rpIdx >= 0) {
    // Slice to end of html — skuPriceList JSON is often >100KB; the old 5000-char
    // window made the brace counter fail and silently fall through to DOM strategy,
    // so multi-variation listings never picked up combos.
    const rpSlice = html.slice(rpIdx);
    const eqIdx = rpSlice.indexOf('=');
    if (eqIdx >= 0) {
      // Count braces to extract the object
      const objStart = rpSlice.indexOf('{', eqIdx);
      if (objStart >= 0) {
        let depth = 0, i = objStart, end = -1;
        for (; i < rpSlice.length; i++) {
          if (rpSlice[i] === '{') depth++;
          else if (rpSlice[i] === '}') { depth--; if (depth === 0) { end = i; break; } }
        }
        if (end > 0) {
          try {
            const rp = JSON.parse(rpSlice.slice(objStart, end + 1));
            const data = rp?.data || rp;
            const tm = data?.titleModule || data?.productDetailPageModule?.titleModule;
            const pm = data?.priceModule || data?.productDetailPageModule?.priceModule;
            if (tm?.subject && pm) {
              return _aeParseFromRunParams(data, aeId, html);
            }
          } catch(e) {}
        }
      }
    }
  }

  // Strategy 2: DOM-based parsing from aliexpress.us rendered HTML
  return _aeParseFromDom(html, aeId);
}

// Parse from window.runParams data object (aliexpress.com)
function _aeParseFromRunParams(data, aeId, html) {
  const tm = data?.titleModule || data?.productDetailPageModule?.titleModule;
  const pm = data?.priceModule || data?.productDetailPageModule?.priceModule;
  const sm = data?.skuModule || data?.productDetailPageModule?.skuModule;
  const im = data?.imageModule || data?.productDetailPageModule?.imageModule;
  const sp = data?.specsModule || data?.productDetailPageModule?.specsModule;
  const sh = data?.shippingModule || data?.productDetailPageModule?.shippingModule;

  const title = (tm?.subject || '').trim();
  const price = parseFloat(pm?.minActivityAmount?.value || pm?.minAmount?.value || 0);
  const images = (im?.imagePathList || []).map(img => img.startsWith('//') ? 'https:' + img : img).filter(Boolean);
  let aeShipping = 0;
  if (!sh?.freightExt?.freightCommitLabel?.toLowerCase().includes('free')) {
    const a = sh?.freight?.freightAmount?.value || sh?.freightExt?.freightAmount?.value;
    if (a) aeShipping = parseFloat(a) || 0;
  }
  const aspects = {};
  for (const s of (sp?.props || [])) { if (s.attrName && s.attrValue) aspects[s.attrName] = s.attrValue; }

  const variations = [], comboPrices = {}, comboInStock = {};
  const variationImages = {}; // dimName → { valueName: imageUrl } — top-level for downstream
  // Build prop→valueId→name lookup for normalizing skuPropIds → "Color|Size" keys
  const _propDims = {}; // propId → { name, valueMap: {valueId → displayName} }
  if (sm?.productSKUPropertyList?.length) {
    for (const prop of sm.productSKUPropertyList) {
      const dimName = prop.skuPropertyName || 'Option';
      const valueMap = {};
      const values = (prop.skuPropertyValues || []).map(v => {
        const name = v.propertyValueDisplayName || v.skuPropertyValueName || '';
        const vid = String(v.propertyValueId || v.skuPropertyValueIdLong || '');
        if (vid) valueMap[vid] = name;
        return { name, image: v.skuColorValue ? 'https:' + v.skuColorValue : '' };
      });
      _propDims[String(prop.skuPropertyId || '')] = { name: dimName, valueMap };
      const varObj = { name: dimName, values: values.map(v => v.name), images: {} };
      if (/color|colour/i.test(dimName)) {
        varObj.images = Object.fromEntries(values.filter(v=>v.image).map(v=>[v.name, v.image]));
        // Also propagate to top-level variationImages so downstream eBay push picks it up
        variationImages[dimName] = varObj.images;
      }
      variations.push(varObj);
    }

    // Determine primary (color) and secondary (size) dim order
    const _primProp = Object.entries(_propDims).find(([,d]) => /color|colour/i.test(d.name))?.[0]
                    || Object.keys(_propDims)[0] || '';
    const _secProp  = Object.keys(_propDims).find(k => k !== _primProp) || '';

    for (const sku of (sm.skuPriceList || [])) {
      const p = parseFloat(sku.skuVal?.skuAmount?.value || sku.skuVal?.activityAmount?.value || 0);
      const qty = parseInt(sku.skuVal?.availQuantity) || 0;
      if (!sku.skuPropIds) continue;
      // Normalize skuPropIds ("14:173;200:1") → "ColorName|SizeName"
      const parts = {};
      for (const pair of sku.skuPropIds.split(';')) {
        const [pid, vid] = pair.split(':');
        if (pid && vid && _propDims[pid]) parts[pid] = _propDims[pid].valueMap[vid] || vid;
      }
      const pVal = parts[_primProp] || Object.values(parts)[0] || '';
      const sVal = _secProp ? (parts[_secProp] || Object.values(parts)[1] || '') : '';
      const comboKey = `${pVal}|${sVal}`;
      if (p > 0) comboPrices[comboKey] = p;
      comboInStock[comboKey] = qty > 0;
    }
  }

  // Determine primary/secondary dim names
  const _primDimObj = variations.find(v => /color|colour/i.test(v.name)) || variations[0];
  const _secDimObj  = variations.find(v => v !== _primDimObj);

  return {
    aeId: aeId || '', title,
    ebayTitle: title.replace(/[^a-zA-Z0-9 ,&\-().']/g,' ').replace(/\s+/g,' ').trim().slice(0,80),
    price, images, aeShipping, deliveryDays: 21,
    aspects, hasVariations: variations.length > 0, variations, comboPrices, comboInStock,
    variationImages,
    comboAsin: {}, // no ASIN concept on AliExpress
    sizePrices: {},
    _isFullyMultiAsin: false,
    _primaryDimName:   _primDimObj?.name || null,
    _secondaryDimName: _secDimObj?.name  || null,
    _source: 'aliexpress',
    inStock: Object.values(comboInStock).some(v => v !== false),
    description: Object.entries(aspects).map(([k,v])=>`${k}: ${v}`).join('\n'),
  };
}

// Parse from rendered aliexpress.us React DOM HTML
// Extracts variations from sku-item-- CSS class patterns
function _aeParseFromDom(html, aeId) {
  // Title: <h1 ...>TITLE</h1>
  const titleM = html.match(/<h1[^>]*>([\s\S]{5,200}?)<\/h1>/);
  const title = (titleM?.[1] || '').replace(/<[^>]+>/g,'').replace(/\s+/g,' ').trim();
  if (!title) return null;

  // Price: look for dollar amounts near product-price classes
  const priceAreaM = html.match(/class="[^"]*price[^"]*"[\s\S]{0,500}/i);
  const priceM = (priceAreaM?.[0] || html).match(/\$(\d+\.\d{2})/);
  const price = parseFloat(priceM?.[1] || 0);

  // Images: main product images from og:image or img tags in product section
  const ogImg = html.match(/<meta[^>]*property="og:image"[^>]*content="([^"]+)"/)?.[1] || '';
  const productImgs = [...html.matchAll(/ae-pic-a1\.aliexpress-media\.com\/kf\/([^"'\s]+\.(?:jpg|png|jpeg|avif|webp))/g)]
    .map(m => 'https://ae-pic-a1.aliexpress-media.com/kf/' + m[1])
    .filter(u => !u.includes('27x27') && !u.includes('favicon'))
    .filter((v,i,a) => a.indexOf(v) === i)
    .slice(0, 8);
  const images = ogImg ? [ogImg, ...productImgs.filter(u=>u!==ogImg)].slice(0,8) : productImgs.slice(0,8);

  // Variations: parse sku-item--property sections
  // Each property section has: title (Color/Size) + items (swatches or text buttons)
  const variations = [];
  const varImagesMap = {};

  // Find all sku-item--property divs using regex on the HTML
  const propBlocks = [...html.matchAll(/class="[^"]*sku-item--property[^"]*"[\s\S]{0,3000}?(?=class="[^"]*sku-item--property|class="[^"]*product-main|<div class="[^"]*ship|$)/g)];

  for (const block of propBlocks.slice(0, 5)) {
    const blockHtml = block[0];

    // Get dimension name from title element
    const titleEl = blockHtml.match(/class="[^"]*sku-item--title[^"]*"[^>]*>([^<]{2,50})/);
    const dimLabel = (titleEl?.[1] || '').replace(/:\s*.*/, '').trim(); // "Color: Multicolor" → "Color"
    if (!dimLabel) continue;

    // Check if image-based (color swatches) or text-based (sizes)
    const hasImgs = blockHtml.includes('sku-item--image');

    if (hasImgs) {
      // Color swatches: extract img src and optional name
      const imgItems = [...blockHtml.matchAll(/class="[^"]*sku-item--(?:image|skus)[^"]*"[\s\S]{0,500}?<img[^>]*src="([^"]+)"[^>]*(?:alt="([^"]*)")?/g)];
      const colorValues = [];
      for (const [, src, alt] of imgItems) {
        const cleanSrc = src.replace(/_220x220q75\.jpg_\.avif|_50x50q\d+\.jpg/g, '');
        // Strip leading numeric prefix: "3 white" → "White", "1 pink" → "Pink"
        const rawName = (alt || '').replace(/^\d+\s+/, '').trim();
        const colorName = rawName || `Color ${colorValues.length + 1}`;
        if (!colorValues.includes(colorName)) {
          colorValues.push(colorName);
          varImagesMap[colorName] = cleanSrc.startsWith('http') ? cleanSrc : 'https:' + cleanSrc;
        }
      }
      if (colorValues.length > 0) variations.push({ name: dimLabel, values: colorValues, images: varImagesMap });
    } else {
      // Text-based sizes/options
      const textItems = [...blockHtml.matchAll(/class="[^"]*sku-item--text[^"]*"[^>]*>[\s\S]{0,100}?<span[^>]*>([\s\S]{0,30}?)<\/span>/g)];
      const sizeValues = textItems.map(m => m[1].replace(/<[^>]+>/g,'').trim()).filter(Boolean);
      // Deduplicate
      const unique = [...new Set(sizeValues)].filter(v => v.length > 0 && v.length < 20);
      if (unique.length > 0) variations.push({ name: dimLabel, values: unique, images: {} });
    }
  }

  // Shipping: check for free shipping text or price
  const freeShipM = html.match(/free\s+shipping/i);
  const shipPriceM = html.match(/shipping[^$]*\$(\d+\.\d+)/i);
  const aeShipping = freeShipM ? 0 : parseFloat(shipPriceM?.[1] || 0);

  // Specs from specification list
  const aspects = {};
  const specMatches = [...html.matchAll(/class="[^"]*specification[^"]*"[\s\S]{0,5000}/)];
  if (specMatches.length) {
    const specHtml = specMatches[0][0];
    for (const m of specHtml.matchAll(/<span[^>]*>([^<]{2,40})<\/span>[\s\S]{0,100}?<span[^>]*>([^<]{1,100})<\/span>/g)) {
      const k = m[1].trim().replace(/:$/, '');
      const v = m[2].trim();
      if (k && v && k.length < 40 && v.length < 100) aspects[k] = v;
    }
  }

  const hasVariations = variations.length > 0;

  // ── Synthesize combo data from base price ────────────────────────────────
  // The .us SPA doesn't expose per-combo prices in the static HTML without
  // clicking each one in the live React app. Without synthesizing combos here,
  // downstream sees comboPrices={} and falls back to a single-variant listing,
  // losing all the dimensions we just parsed. Apply the base buy-box price to
  // every (primary, secondary) combo and assume in-stock — this matches what
  // AE typically shows in its UI before any combo selection anyway.
  const _comboPrices  = {};
  const _comboInStock = {};
  if (hasVariations && price > 0) {
    const _primVar = variations.find(v => /color|colour/i.test(v.name)) || variations[0];
    const _secVar  = variations.find(v => v !== _primVar);
    const primVals = _primVar?.values || [''];
    const secVals  = _secVar?.values  || [''];
    for (const pv of primVals) {
      for (const sv of secVals) {
        const key = `${pv}|${sv}`;
        _comboPrices[key]  = price;
        _comboInStock[key] = true;
      }
    }
  }

  console.log(`[aeParseFromDom] ${aeId}: "${title.slice(0,40)}" $${price} vars=${variations.map(v=>v.name+':'+v.values.length).join(',')} combos=${Object.keys(_comboPrices).length}`);

  const _primVarDom = variations.find(v => /color|colour/i.test(v.name)) || variations[0];
  const _secVarDom  = variations.find(v => v !== _primVarDom);
  return {
    aeId, title,
    ebayTitle: title.replace(/[^a-zA-Z0-9 ,&\-().']/g,' ').replace(/\s+/g,' ').trim().slice(0,80),
    price, images, aeShipping, deliveryDays: 21,
    aspects, hasVariations, variations,
    comboPrices: _comboPrices, // synthesized from base price (DOM strategy can't get per-combo)
    comboInStock: _comboInStock, comboAsin: {}, sizePrices: {},
    _isFullyMultiAsin: false,
    _primaryDimName:   _primVarDom?.name || null,
    _secondaryDimName: _secVarDom?.name  || null,
    _source: 'aliexpress',
    inStock: true, // AliExpress US warehouse = assume in-stock
    description: Object.entries(aspects).map(([k,v])=>`${k}: ${v}`).join('\n'),
    variationImages: hasVariations ? { [_primVarDom?.name || 'Color']: varImagesMap } : {},
  };
}


// ══════════════════════════════════════════════════════════════════════════════
// WEBSTAURANTSTORE SCRAPER
// All self-contained. Touches no Amazon/AliExpress code.
// ══════════════════════════════════════════════════════════════════════════════

// ── Estimate shipping cost from productTemplates data ─────────────────────────
// WS does not expose dollar shipping in HTML — it's ZIP-dependent.
// We use a tiered model based on hasFreeShipping + isCommonCarrier + weightInPounds.
// User can override defaults via wsSettings in DropSync Settings.
function wsEstimateShipping(tpl, wsSettings = {}) {
  if (!tpl) return 0;
  if (tpl.hasFreeShipping) return 0;
  if (tpl.isCommonCarrier) return parseFloat(wsSettings.wsFreightEstimate || 200);

  const lbs = parseFloat(tpl.weightInPounds || 0);
  const flatMin = parseFloat(wsSettings.wsShippingFlat || 18);  // FedEx minimum
  const perLb   = parseFloat(wsSettings.wsShippingPerLb || 1.5);

  if (lbs <= 0) return flatMin;
  if (lbs < 1)  return flatMin;                        // < 1 lb → flat minimum
  if (lbs <= 5) return Math.max(flatMin, lbs * perLb + 10);
  if (lbs <= 20) return Math.max(flatMin, lbs * perLb + 8);
  return Math.max(flatMin, lbs * perLb + 5);
}

// ── Extract the embedded JSON comment from WS HTML ────────────────────────────
function wsExtractComment(html, key) {
  if (!html) return null;
  let pos = 0;
  while (true) {
    const start = html.indexOf('<!--{', pos);
    if (start === -1) break;
    const end = html.indexOf('}-->', start);
    if (end === -1) break;
    try {
      const data = JSON.parse(html.slice(start + 4, end + 1));
      if (!key || data[key] !== undefined) return data;
    } catch(e) {}
    pos = end + 4;
  }
  return null;
}

// ── Build full CDN image URL from WS image path ───────────────────────────────
function wsImageUrl(path) {
  if (!path) return '';
  if (path.startsWith('http')) return path;
  const base = 'https://cdnimg.webstaurantstore.com';
  // Ensure /large/ size
  const large = path.replace(/\/(thumbnails|small|medium|extra_large|xxl|landscape)\//, '/large/');
  return base + (large.startsWith('/') ? large : '/' + large);
}

// ── Scrape a single WebstaurantStore product page ─────────────────────────────
function wsScrapeProduct(html, sourceUrl, wsSettings = {}) {
  if (!html || html.length < 1000) return null;

  // Extract productTemplates from HTML comment
  const pageData = wsExtractComment(html, 'productTemplates');
  if (!pageData?.productTemplates?.length) {
    console.warn('[wsScrape] no productTemplates found in HTML');
    return null;
  }
  const tpl = pageData.productTemplates[0];

  // ── Basic fields ─────────────────────────────────────────────────────────
  const title       = (tpl.title || tpl.name || '').replace(/\s+/g, ' ').trim();
  const price       = parseFloat(tpl.price?.price || 0);
  const isInStock   = tpl.isInStock !== false;
  const brand       = tpl.primaryBrand?.name || tpl.brand || '';
  const itemNumber  = (tpl.itemNumber || '').toUpperCase();
  const description = tpl.description || '';
  const shipping    = wsEstimateShipping(tpl, wsSettings);
  const hasFreeShip = !!tpl.hasFreeShipping;
  const isFreight   = !!tpl.isCommonCarrier;
  const weightLbs   = parseFloat(tpl.weightInPounds || 0);
  const stockMsg    = tpl.stockMessage || '';
  const unitsPerPkg = tpl.unitsPerPackaging || 1;

  // ── Images ───────────────────────────────────────────────────────────────
  const images = [];
  for (const img of (tpl.productImages || [])) {
    const url = wsImageUrl(img.large || img.extraLarge || img.landscape || '');
    if (url && !images.includes(url)) images.push(url);
  }
  // Fallback: JSON-LD ProductGroup images
  if (!images.length) {
    try {
      const jsonLdMatch = html.match(/<script[^>]*application\/ld\+json[^>]*>([\s\S]*?)<\/script>/g) || [];
      for (const s of jsonLdMatch) {
        const d = JSON.parse(s.replace(/<\/?script[^>]*>/g, '').trim());
        if (d['@type'] === 'ProductGroup' || d['@type'] === 'Product') {
          const imgs = Array.isArray(d.image) ? d.image : (d.image ? [d.image] : []);
          for (const img of imgs) { if (img && !images.includes(img)) images.push(img); }
          for (const v of (d.hasVariant || [])) {
            if (v.image && !images.includes(v.image)) images.push(v.image);
          }
        }
      }
    } catch(e) {}
  }

  // ── Specs (aspects) ───────────────────────────────────────────────────────
  const aspects = {};
  if (brand) aspects.Brand = brand;
  const specs = tpl.specificationTable?.specifications || [];
  for (const spec of specs) {
    const name = spec.classification || spec.name || '';
    const value = (spec.types || []).map(t => t.value).filter(Boolean).join(', ');
    if (name && value && name.length < 50 && value.length < 200) aspects[name] = value;
  }
  // Also grab breadcrumbs for category context
  const breadcrumbs = (tpl.breadCrumbs || []).map(b => b.displayText || b).filter(Boolean);

  // ── Highlights / bullets ──────────────────────────────────────────────────
  const bullets = Array.isArray(tpl.highlights) ? tpl.highlights.filter(Boolean) : [];

  // ── Variations from variationMembership ───────────────────────────────────
  // IMPORTANT: WebstaurantStore's variationGroups do NOT represent orthogonal
  // axes (color × size) of one product like Amazon does. Each variationGroupItem
  // is a separate sibling product page in the same family, with its own price,
  // stock, and image. The previous implementation cross-multiplied two groups
  // into a fake matrix, producing combos that don't exist as real SKUs and
  // assigning the same price to every cell in a row.
  //
  // Correct approach: flatten ALL sibling items from ALL groups into a single
  // "Option" dimension where each value = one real sibling product. Each value
  // carries its own real price, stock, and image. This produces an honest
  // multi-variation eBay listing without invented SKUs.
  const varGroups = tpl.variationMembership?.variationGroups || [];
  const variations = [];
  const comboPrices = {};
  const comboInStock = {};
  const variationImages = {};
  let _primaryDimName = null;
  let _secondaryDimName = null;

  if (varGroups.length > 0) {
    // Use the first group's optionName as the dimension label, fall back to "Option"
    _primaryDimName = varGroups[0].optionName || 'Option';
    const primVarImages = {};
    const primValues = [];
    const seenLabels = new Set();

    // Walk every group, every item — sibling products in the family
    for (const group of varGroups) {
      for (const item of (group.variationGroupItems || [])) {
        const vText = (item.variationText || item.displayText || '').trim();
        if (!vText || seenLabels.has(vText)) continue;
        seenLabels.add(vText);

        const vPrice = parseFloat(item.productPrice?.price || price || 0);
        const vStock = item.isInStock !== false;
        const vImg   = wsImageUrl(item.seoImage || item.thumbnail || '');
        if (vImg) primVarImages[vText] = [vImg];

        primValues.push({ value: vText, price: vPrice, inStock: vStock, enabled: vStock });

        // Single-dimension combo key format: "value|"
        const comboKey = `${vText}|`;
        comboPrices[comboKey]  = vPrice;
        comboInStock[comboKey] = vStock;
      }
    }

    if (primValues.length > 0) {
      variations.push({ name: _primaryDimName, values: primValues });
      variationImages[_primaryDimName] = primVarImages;
    }
  }

  const hasVariations = variations.length > 0 && (variations[0].values?.length > 1 || variations.length > 1);

  // ── Build best price ──────────────────────────────────────────────────────
  let bestPrice = price;
  if (Object.keys(comboPrices).length) {
    const stockedPrices = Object.entries(comboPrices)
      .filter(([k]) => comboInStock[k] !== false)
      .map(([,p]) => p).filter(p => p > 0);
    if (stockedPrices.length) bestPrice = Math.min(...stockedPrices);
  }

  // ── Return product object ─────────────────────────────────────────────────
  return {
    title,
    price:  bestPrice,
    cost:   bestPrice,
    _wsShipping: shipping,
    _wsItemNumber: itemNumber,
    _wsHasFreeShipping: hasFreeShip,
    _wsIsFreight: isFreight,
    _wsWeightLbs: weightLbs,
    _wsStockMessage: stockMsg,
    _wsUnitsPerPackaging: unitsPerPkg,
    images: images.slice(0, 12),
    bullets,
    description,
    aspects,
    breadcrumbs,
    hasVariations,
    variations,
    comboPrices,
    comboInStock,
    comboAsin:    {},  // no ASIN concept on WS — comboPrices/comboInStock are enough
    sizePrices:   {},
    variationImages,
    inStock: isInStock || Object.values(comboInStock).some(v => v !== false),
    _primaryDimName,
    _secondaryDimName,
    _extraDimNames: [],
    _isFullyMultiAsin: false,
    _source:       'webstaurant',
    sourceUrl:     sourceUrl || '',
  };
}

// ── Extract product listing from WS category/search page ─────────────────────
function wsExtractListings(html) {
  if (!html) return [];
  const data = wsExtractComment(html, 'products');
  if (!data?.products?.length) return [];

  return data.products.map(p => {
    // URL: search pages use 'link' field (e.g. "/choice-16-oz-clear.../500CC16.html")
    const link = p.link || p.itemPageUri || p.canonicalUrl || '';
    const fullUrl = link.startsWith('http') ? link : 'https://www.webstaurantstore.com' + link;

    // Image: search pages use primaryImagePath (relative CDN path)
    const imgPath = p.primaryImagePath || p.image?.path || p.image?.large || p.imageUrl || '';
    const imgUrl  = imgPath ? wsImageUrl(imgPath) : (typeof p.image === 'string' ? p.image : '');

    // Title: 'header' is the display name, fallback to description/name
    const title = (p.header || p.description || p.name || p.title || '').replace(/\s+/g, ' ').trim().slice(0, 200);

    // Brand: direct string in search results
    const brand = (typeof p.brand === 'string' ? p.brand : p.brand?.name) || p.primaryBrand?.name || '';

    return {
      title,
      price:          parseFloat(p.price?.price || p.price || 0),
      isInStock:      p.isInStock !== false,
      brand,
      itemNumber:     (p.itemNumber || '').toUpperCase(),
      url:            fullUrl,
      image:          imgUrl,
      hasFreeShipping: !!p.hasFreeShipping,
      weightInPounds:  parseFloat(p.weightInPounds || 0),
      rating:         p.overallRating || p.oneToFiveStarRating || 0,
      reviewCount:    p.productReviewCount || p.reviewCount || 0,
    };
  }).filter(p => p.url && p.title && p.price > 0);
}

// ── AliExpress API action handler ─────────────────────────────────────────────
if (typeof module !== 'undefined' && module.exports) {
  // Exposed for use in the Express route handler
  module.exports.aeExtractListings = aeExtractListings;
  module.exports.aeScrapeProduct = aeScrapeProduct;
  module.exports.AE_DEPT_URLS = AE_DEPT_URLS;
  module.exports.wsScrapeProduct   = wsScrapeProduct;
  module.exports.wsExtractListings = wsExtractListings;
  module.exports.setRelayHandle    = setRelayHandle;
}
