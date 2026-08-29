# VeRO: how eBay actually reports removals, and what to do

Written after a second suspension (7 days → 10 days). The escalation matters:
eBay's published pattern is warning → temporary restriction → longer restriction
→ permanent. The next step after 10 days is the one that ends the account.

---

## 1. Every channel eBay uses, and which we can read

| Channel | What it carries | Machine-readable? | Were we using it? |
|---|---|---|---|
| **`GetSellerList` / `GetItem`** → `SellingStatus.AdminEnded`, `ItemPolicyViolation.PolicyID` / `PolicyText` | **Which listing eBay ended and why** | **Yes — definitive** | ❌ no |
| `SellingStatus.ListingOnHold` | Listing suspended but not ended | Yes | ❌ no |
| `GetMyMessages` | Notice text, sometimes with item IDs | Partly — needs parsing | ✅ yes |
| Email | Same as messages | No | ❌ |
| Platform Notifications (`ItemSuspended`) | Real-time push on suspension | Yes, needs a webhook | ❌ not yet |
| Seller Hub → Listings → Ended | Same data as GetSellerList | Manual | ❌ |
| Sell **Compliance API** | Listing-quality issues only — **not** VeRO/IP | Yes, but wrong data | n/a |
| **VeRO API** | For rights owners to FILE reports — sellers cannot read reports against them | n/a | n/a |

**The key finding:** eBay publishes the removal reason through the Trading API on
the listing itself. We were parsing message text and guessing which listing it
referred to, which missed any notice without an item ID — including the account
restriction notice that started this.

## 2. What changed in the code

**`vero-enforce.js` (new) — authoritative detection**
- Sweeps `GetSellerList` for `AdminEnded`, `ListingOnHold`, `ItemPolicyViolation`
- Records item ID, policy ID and the exact violation text
- Flags each affected listing `do_not_relist` (sync and push both honour it)
- Deletes it from the sync queue so nothing republishes it
- **Extracts the rights owner from the violation text and blocks that brand**
- Runs automatically every 4 hours, or on demand:
  `POST /api/vero/scan-violations { access_token, days }`

**`vero.js` — hardened pre-import screening**
- Screened the TITLE only, so a product whose brand appeared just in Amazon's
  brand field or byline passed straight through. Now screens brand, byline and
  manufacturer as well.
- **Any brand that has already caused a removal on this account is refused
  outright**, regardless of score. The same rights owner reports repeatedly.

## 3. What the code cannot fix

This is the part that matters most, and no amount of detection changes it.

**Every removal so far has been VeRO — a rights owner reporting the listing.**
The listings in the logs are adidas, DREAM PAIRS, Carter's, Pampers: brand-name
products listed with **Amazon's product photography and copy**. Rights owners
report that on sight, and they are correct to — the images belong to them.
Detection only tells you after the strike has landed.

Three things drive the reports, in order:

1. **Amazon's images.** Copyright infringement, provable, trivially detected by
   the brands' monitoring services. This is the single biggest trigger.
2. **Brand names in titles** on listings with no authorisation to resell.
3. **Copied Amazon copy** — same copyright issue as the images.

**A durable fix is a catalogue decision, not a code change:**

- **Stop listing VeRO-participant brands.** The screening now blocks them, which
  will reject a meaningful share of your imports. That rejection is the point.
- **Stop reusing Amazon images** on anything branded. Own photography or
  supplier-provided images only.
- **Prefer generic/unbranded goods** — the tablecloths, kitchen tools and
  organisers in your catalogue have never been reported.

Also worth knowing: eBay's dropshipping policy permits fulfilment from a
wholesale supplier but not buying from another retailer that ships directly to
your buyer. That sits underneath the VeRO problem as separate exposure.

## 4. Do this now, in order

1. **Deploy** `vero-enforce.js`, `vero.js`, `server.js`, `ebay.js`.
2. **Run the violation scan** with a 120-day window — this is the first time you
   will have a complete list of what eBay removed and why:
   ```js
   fetch(S.serverUrl + '/api/vero/scan-violations', {method:'POST',
     headers:{'Content-Type':'application/json'},
     body: JSON.stringify({access_token: S.token, days: 120})})
     .then(r=>r.json()).then(console.log)
   ```
   Every affected listing is flagged and every offending brand blocked.
3. **Review** `GET /api/vero/violations` — the brand list there is your real
   risk profile, derived from what has actually been reported.
4. **Run the catalogue audit** at `/vero-report` and end the CRITICAL tier
   yourself rather than waiting for reports.
5. **Keep syncing paused** until the restriction lifts. Nothing should publish.
6. **Decide on the catalogue.** With two suspensions on file, continuing to list
   branded goods with Amazon imagery makes a third one likely, and the third is
   where accounts are lost.
