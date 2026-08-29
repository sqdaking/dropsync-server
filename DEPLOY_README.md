# DropSync — complete update package (July 2026)

Everything in this folder is a **drop-in replacement**. All syntax-checked and
load-tested. Database schema migrates itself on boot — no manual SQL.

---

## Files to update

### Repo root (`sqdaking/dropsync-server`)

| File | Status | What's in it |
|---|---|---|
| `ebay.js` | **replace** | smartSync rewrite, bulk price/qty updates, variant matching fixes, concurrency race fix, multi-account resolver, VeRO sync gate |
| `dropsync.html` | **replace** | in-stock detection fix, fresh variant maps, account scoping, dead proxies removed, rotation caps |
| `server.js` | **replace** | account-scoped REST endpoints, `/api/amazon` disabled, VeRO routes + message poller |
| `db.js` | **replace** | `account_id` on products/logs/settings, composite settings PK, `claimDefaultAccount` |
| `vero.js` | **NEW — git add** | VeRO risk scoring, audit report, do-not-relist enforcement |
| `vero-inbox.js` | **NEW — git add** | reads eBay My Messages, auto-flags listings named in notices |
| `logtail.js` | **NEW — git add** | in-app live log tail + Railway rate-limit protection |
| `neutralize.js` | **NEW — git add** | strips variant-specific claims from multi-variant descriptions |
| `descrefresh.js` | **NEW — git add** | repairs existing listing descriptions in place (no re-push) |

```bash
git add vero.js vero-inbox.js logtail.js neutralize.js descrefresh.js
git add -A
git commit -m "smartsync fixes, multi-account, VeRO protection"
git push
```

### Chrome extension (separate — not in the repo)

`dropsync-extension/` → v2.0.1. Install via `chrome://extensions` →
Developer mode → **Load unpacked** → select the folder containing
`manifest.json`. Do this in **every** Chrome profile.

---

## Deploy order (order matters)

1. **Push the repo files.** Railway auto-deploys. Watch the logs for:
   - `[DB] settings PK migrated to (account_id, key)`
   - `[DB] Schema ready`
   - `[vero] risk screening ready`
   - `[vero-inbox] ready`
2. **Hard-refresh the DropSync tab** (Ctrl+Shift+R). Console must show
   `[dropsync] build 2026-07-21.3`. If it doesn't, the refresh didn't take —
   clear site data and reload.
3. **Reload the extension** in each Chrome profile. Popup must read **v2.0.1**.
4. **Reconnect eBay** (Settings → Connect), original account first. The status
   bar should now show `Connected to eBay as <username>`.
5. **Claim legacy data** — click the link in Settings, once, from the original
   account's browser. Assigns all pre-multi-account rows to that username.
6. **Purge the ASIN cache** (earlier runs cached bad `inStock:false` values).
   In the DropSync tab console:
   ```js
   fetch(S.serverUrl + '/api/ebay?action=cache_clear', {method:'POST',
     headers:{'Content-Type':'application/json'}, body: JSON.stringify({all:true})})
     .then(r=>r.json()).then(console.log)
   ```
7. **Run the VeRO audit** — open `<your-railway-url>/vero-report`, click
   **Scan eBay messages for notices** (60-day window), then review the ranked
   kill-list and flag what you're removing.

> **While your account is restricted:** keep auto-sync and the relay paused.
> Nothing should publish until the 7 days are up.

---

## Environment variables (all optional)

| Var | Default | Effect |
|---|---|---|
| `MAX_VARIANTS_PER_SYNC` | `15` | variants fresh-fetched per listing per cycle (rotation slice) |
| `CLEANUP_CHEAP_PRICE` | `15` | delete variants at/below this price; `0` disables |
| `VERO_SCREEN` | `block` | `block` \| `warn` \| `off` — push-time IP screening |
| `TWO_PHASE_SYNC` | `on` | `off` = single-pass (no zero-first phase) |
| `AUTO_DELETE_VARIANTS` | `off` | `on` = allow variant deletion after repeated strikes. Off = zero only (reversible) |
| `MAX_PUSH_VARIANTS` | `25` | max variants a NEW listing is created with (existing listings unaffected; repush to shrink one) |
| `ALWAYS_FETCH` | `off` | `on` = server never fills gaps from cache and never tells the extension to skip "fresh" ASINs (the browser already forces this per-request) |
| `STALE_PRICE_HOURS` | `72` | cached prices older than this are held at qty 0 instead of selling; `0` disables |
| `ZERO_MIN_COVERAGE` | `0.5` | below this data coverage, only variants with fresh data are zeroed |
| `DELETE_STRIKES` | `3` | failed syncs (1h apart, 24h+ span) required before a variant may be deleted |
| `LOG_LEVEL` | `info` | `debug` = everything to Railway; `warn` = warnings only. Never affects `/logs-live` |
| `LOG_STDOUT_PER_SEC` | `200` | stdout cap, kept under Railway's 500/sec limit |
| `LOG_RING_SIZE` | `3000` | lines kept in memory for the live tail |

---

## New endpoints

| Endpoint | Purpose |
|---|---|
| `GET /desc-refresh` | preview/apply variant-neutral descriptions in place |
| `POST /api/desc/refresh` | `{access_token, apply, publish, limit, offset}` |
| `GET /logs-live` | live log tail (SSE, instant, no Railway lag or drops) |
| `GET /api/logs/tail?n=500&q=regex` | JSON log snapshot for grep/scripting |
| `GET /vero-report` | full audit UI (risk table, flagging, CSV, message scan) |
| `GET /api/vero/audit` | JSON risk scan of all listings |
| `POST /api/vero/flag` | `{ids[], flag}` → set/clear do-not-relist |
| `POST /api/vero/brands` | `{brand, tier}` → extend the blocklist |
| `GET /api/vero/export.csv` | kill-list as CSV |
| `POST /api/vero/scan-messages` | `{access_token, days}` → parse eBay notices |
| `GET /api/vero/notices` | stored notices with review status |
| `POST /api/claim-default-account` | one-time legacy data migration |

---

## What changed and why

### Sync correctness
- **Cache was never written** in browser-only mode → the 7-day ASIN cache sat
  empty, so any variant not fetched that exact cycle went stale. Fresh browser
  prices are now cached.
- **Variants 16+ were unreachable** — the variant cap truncated the ASIN list
  *before* the rotation offset applied. Rotation now covers every variant.
- **In-stock detection was matching page-wide** — `outOfStockBuyBox` and
  `Join the waitlist` appear in Amazon's bundled JS on every page, so every
  variant was flagged OOS and pushed `qty=0`. Now scoped to the buy-box /
  availability region with positive signals (Add to Cart, "In Stock") and
  negative signals ("See available options", "Currently unavailable",
  "Out of stock", "Available from these sellers", waitlist).
- **Wrong prices across variants** — three causes, all fixed:
  1. multi-word single values (`1 Little Kid`) were misclassified as compound
     slugs, letting one size's ASIN match every colour;
  2. the aspect upgrade fell back to a single value and **overwrote** correct
     matches (the "N corrected" lines were N variants being smeared);
  3. eBay's 50-char SKU cap truncates colour names — added reverse tail
     matching, accepted only when exactly one ASIN matches.
- **Concurrency race** — slug maps lived in `global.__smartSync*` while syncs
  run concurrently, so one listing could overwrite another's map mid-flight and
  assign the wrong ASINs. Now per-request locals.
- **Partial-map guard** — variants are only zeroed when the assembled map
  actually covers the listing. On partial data they're logged `UNRESOLVED` and
  left untouched instead of being killed.
- **Cleanup no longer deletes variants it just repriced** (it was comparing
  against pre-update prices).

### API efficiency
- `bulkUpdatePriceQuantity` (25 SKUs/call) replaces the per-variant
  GET→PUT→GET→PUT chain; the old path survives only as a fallback.
- Publish only runs when the fallback ran — saves a revision against eBay's
  250/listing/day cap on every sync.
- Offer IDs are cached and reused, skipping discovery entirely.

### Block avoidance
- Dead CORS proxies and the Railway `/api/amazon` proxy removed; server-side
  Amazon fetching is hard-refused.
- Extension budgets **requests per minute** (8 → 30 adaptive, halves on block)
  rather than listings per tick.
- Parent-page bulk extraction + server-supplied `freshAsins` (cached <20h)
  cut request volume sharply.

### Multi-account
- eBay username resolved from the token via Trading API `GetUser`.
- `account_id` on `products`, `logs`, `settings`, `relay_state`; settings PK is
  now `(account_id, key)` so accounts can't overwrite each other's tokens.
- Cross-account guard returns 403 if a token and a listing's account disagree.
- ASIN cache stays shared — Amazon prices are account-independent.
- Use one Chrome profile per eBay account.

### VeRO / IP protection
- Risk scoring on brand (tiered lists), Amazon-hosted images, and copied
  Amazon copy, with negative-context guards so "Apple Cider Vinegar" and
  "Polo Shirt" don't false-positive.
- `do_not_relist` is enforced in three places: smartSync refuses, the relay
  queue excludes, and the relay_state row is deleted.
- Push-time screening blocks CRITICAL/HIGH imports before listing.
- Message scanner parses eBay notices, auto-flags the named listings, and adds
  the offending brand to the permanent blocklist.

**Flagging never ends a listing on eBay** — that stays manual in Seller Hub, on
purpose.
