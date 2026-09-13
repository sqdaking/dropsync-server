/* Run in the DropSync tab console.
   Fetches each ASIN of one listing INDIVIDUALLY and prints the price the
   extractor reads from that ASIN's own page. This separates the two remaining
   possibilities:
     • 16 distinct prices here, identical on eBay  → the server is writing wrong
     • identical prices here                        → Amazon is serving the same
       page for every ASIN, or the extractor is reading the parent's buy box  */
(async () => {
  const p = (S.products || []).find(x => (x.ebaySku || '').includes('P6Q5M'));
  if (!p) return console.error('listing not in this tab');

  const html = await fetchAmazonInBrowser(p.sourceUrl);
  if (!html) return console.error('parent fetch failed');

  // ASINs straight from Amazon's own variant map
  const m = html.match(/"dimensionToAsinMap"\s*:\s*(\{[^}]{0,20000}\})/);
  const dta = m ? JSON.parse(m[1]) : {};
  const asins = [...new Set(Object.values(dta))].filter(a => /^[A-Z0-9]{10}$/.test(a));
  console.log(`checking ${asins.length} ASINs individually…`);

  const rows = [];
  for (const asin of asins) {
    const r = await window.dsExt.fetchAmazon(`https://www.amazon.com/dp/${asin}?th=1&psc=1`);
    if (!r?.ok || !r.html) { rows.push({ asin, price: null, note: r?.reason || 'fetch failed' }); continue; }
    const canon = (r.html.match(/"currentAsin"\s*:\s*"([A-Z0-9]{10})"/) || [])[1];
    const d = _browserExtractAsinData(r.html, asin);
    rows.push({
      asin,
      price: d ? d.price : null,
      inStock: d ? d.inStock : null,
      servedAsin: canon,
      sameAsRequested: canon === asin,
    });
    await new Promise(x => setTimeout(x, 400));
  }
  console.table(rows);
  const prices = rows.map(r => r.price).filter(Boolean);
  console.log('distinct prices:', new Set(prices).size, 'of', prices.length);
  const wrongPage = rows.filter(r => r.servedAsin && !r.sameAsRequested).length;
  if (wrongPage) console.warn(`${wrongPage} ASIN(s) were served a DIFFERENT variant's page by Amazon`);
})()
