# DropSync Upgrade — July 2026
## SmartSync / monitoring fixes + Multi-account eBay support

This upgrade touches **ebay.js, server.js, db.js, dropsync.html** and fully
replaces the Chrome extension (**dropsync-extension/ v2.0**). All database
schema changes are automatic (`ALTER TABLE ... IF NOT EXISTS` on boot) — no
manual SQL needed.

---

## Part 1 — Why syncs were making mistakes (root causes fixed)

### 1. Fresh browser data was never cached (the big one)
`smartSync`'s `clientAsinData` path **read** from the 7-day ASIN cache but
never **wrote** to it. Since the server no longer fetches Amazon itself (the
only code path that wrote to the cache), the cache was permanently empty in
browser-only mode. Result: "cache-fill for uncovered variants" filled nothing,
and every variant the browser didn't fetch that exact cycle went stale.
**Fixed:** every fresh price from the browser/extension is now written to the
cache (except the suspect `$9.99` value, per your rule).

### 2. Variants beyond #15 could never sync
`MAX_VARIANTS_PER_SYNC` truncated the ASIN list to the first 15 **before** the
rotation offset was applied, so the offset always reset to 0 against a 15-item
list. On a 60-variant listing, variants 16–60 were permanently frozen.
**Fixed:** the server now considers **all** variants every cycle (fresh data +
7-day cache), and `MAX_VARIANTS_PER_SYNC` is only a **rotation hint**: it tells
the fetcher which slice of 15 to fresh-fetch next cycle. Over N cycles every
variant gets refreshed; between refreshes the (now working) cache covers it.

### 3. eBay updates rewritten to `bulkUpdatePriceQuantity`
The old STEP 5 did GET offer → PUT offer → GET inventory item → PUT inventory
item for **every variant**, plus a publish **every cycle** (~4+ API calls and
multiple revisions per listing per sync). eBay caps revisions at **250 per
listing per calendar day** — frequent multi-variant syncs were eating it.
**Fixed:** one `bulk_update_price_quantity` call updates 25 variant SKUs
(price + offer qty + ship-to-location qty) at once. The old per-offer path
survives only as a fallback for entries the bulk call rejects, and publish
only runs when that fallback ran.

### 4. Cleanup was deleting variants it had just fixed
The "delete any variant ≤ $15" cleanup compared against the **pre-update**
offer price. A variant repaired from $9.99 → $47.99 in the same cycle still
matched "≤ $15" and got deleted seconds after being fixed.
**Fixed:** variants successfully repriced this cycle are exempt. The threshold
is now env-tunable: `CLEANUP_CHEAP_PRICE` (default 15, set `0` to disable).

### 5. Background syncs used the wrong markup
`relay_result` never carried your settings, so extension-driven syncs priced
everything at the hardcoded defaults (23% / $2 / qty 1) regardless of what you
set. **Fixed:** the UI now registers each listing with its own
markup/handling/qty into `relay_state`; the server uses those for every
background sync.

### 6. Background syncs re-discovered offers every cycle
Despite offer IDs being cached client-side, the relay path never passed them —
every background sync ran the full discovery chain (group GET + offer scans +
per-SKU GETs). **Fixed:** `relay_state` stores `offer_ids`; smartSync returns
`offerIdsBySku` after every sync and both the browser and relay persist it.

### 7. Claim-lock on the batch queue
Overlapping ticks (a slow fetch cycle + the next alarm) could hand the same
listing to two workers. `relay_next_batch` now marks rows `last_dispatched`
and skips anything dispatched in the last 5 minutes (`FOR UPDATE SKIP LOCKED`).

### 8. Dead/blocked fetch paths removed (block avoidance + policy)
- CORS proxies (codetabs / corsproxy.io / allorigins) **removed** from both
  browser fetch chains — shared datacenter IPs are 100% blocked by Amazon and
  each attempt wasted up to 9s and leaked your product URLs.
- Railway `/api/amazon` proxy **disabled** (HTTP 410) — it fetched Amazon from
  Railway's datacenter IP, violating the browser-only rule.
- `fetchAmazonMini` now hard-refuses to run without a proxy agent — the server
  can no longer hit Amazon from its own IP under any code path.
- Everything Amazon now goes through the extension on your residential IP,
  with the user's real session cookies (`credentials: 'include'`).

### 9. relay_blocked no longer pollutes the table
It used to INSERT rows with empty `source_url` for unknown SKUs, which then
never matched the `LIKE '%amazon%'` filter — dead rows forever. Now UPDATE-only.

---

## Part 2 — Multi-account eBay (no more confusion)

### How identity works
- On eBay connect, the server resolves your **eBay username** via Trading API
  `GetUser` (works with your existing token — no new OAuth scope, no
  re-consent) and returns it as `accountId` in the auth payload.
- The browser stores it as `S.accountId`, shows it in Settings
  ("Connected to eBay as **username**"), and transparently appends
  `?account=<username>` to every `/api/products`, `/api/settings`, `/api/logs`
  call (one fetch wrapper — no scattered call-site changes).
- Every relay call carries the access token; the **server independently
  resolves the account from the token** and refuses cross-account operations
  (e.g. syncing a listing registered to account A with account B's token
  returns 403).

### What got scoped
| Table | Change |
|---|---|
| `products` | `account_id` column + all queries scoped |
| `settings` | `account_id` column, PK migrated to `(account_id, key)` — accounts can no longer overwrite each other's refresh tokens/markup |
| `logs` | `account_id` column + scoped |
| `relay_state` | `account_id` + per-listing settings + offer IDs + rotation offset + claim-lock |
| `asin_cache` | **intentionally shared** — an ASIN's Amazon price is account-independent, so both accounts benefit from each other's fetches |

### One-time migration for your existing account
All pre-upgrade rows sit under `account_id='default'`. After deploying:
1. Open DropSync **in the browser of your ORIGINAL account** and reconnect
   eBay (Settings → Connect) so `S.accountId` gets set.
2. In Settings you'll see: *"Claim this server's legacy data for this
   account"* — click it once. All `default` rows (products, settings, logs,
   relay state) are assigned to that username. The server verifies the token
   actually belongs to the claimed username before migrating.
3. The second account then connects from its own browser profile and starts
   with a clean, isolated workspace.

### Recommended setup for 2 accounts
Use **one Chrome profile per eBay account** (Chrome → profile switcher → Add):
- Each profile has its own cookies (important — eBay dislikes linked accounts
  sharing sessions), its own DropSync tab, and its own copy of the extension
  holding **that account's** token.
- The server hands each extension instance only its own account's listings,
  so background syncs can never cross accounts even if both run 24/7.
- Both profiles share your residential IP for Amazon — that's fine, the fetch
  budget in each extension keeps the combined rate human-shaped. If you run
  both profiles simultaneously, consider lowering each budget's ceiling
  (they're independent) — or just let the adaptive limiter find the level.

---

## Part 3 — New Chrome extension (v2.0)

Replace the old extension entirely: `chrome://extensions` → remove old →
"Load unpacked" → select the new `dropsync-extension/` folder. Do this in
**each** Chrome profile.

Key behavior changes:
- **Fetch-budget limiter**: budgets Amazon *requests per minute* (start 8,
  +2 per 25 clean fetches, cap 30; any block → halve + 5-min cooldown;
  3 blocks in 15 min → pinned to 3). The old "listings per tick" model could
  fire hundreds of requests a minute — the #1 cause of your blocks.
- **Parent-page bulk extraction**: variant prices are pulled from the parent
  page's embedded twister data first; only uncovered ASINs get individual
  fetches. One fetch often covers most of a listing.
- **Skips fresh ASINs**: the server sends `freshAsins` (cached <20h) with each
  batch — those are never re-fetched.
- **Variant rotation**: fetches the slice `[asinOffset, +15]` per cycle;
  the server advances the offset after each sync.
- **Per-ASIN 30-min cooldown** and jittered pacing (1.2–2.8s) as before.
- **Multi-account**: `dsExt.setToken(token, accountId)` — the DropSync tab
  pushes both automatically; the popup shows which account this profile's
  bridge is serving.

Open the DropSync tab once after installing so the extension receives the
token + account + server URL.

---

## Part 4 — Deploy checklist

1. `git add -A && git commit -m "smartsync fixes + multi-account" && git push`
   (Railway auto-deploys; schema migrates itself on boot — watch logs for
   `[DB] settings PK migrated` and `[relay] schema` lines).
2. Hard-refresh the DropSync tab (**Ctrl+Shift+R**).
3. Reconnect eBay in Settings (original account's browser first) → click
   **Claim legacy data**.
4. Replace the extension in each Chrome profile (remove old, load `dropsync-extension/`).
5. Open each account's DropSync tab once (pushes token/account to its extension).
6. Optional env vars:
   - `MAX_VARIANTS_PER_SYNC` (default 15) — variants fresh-fetched per cycle
   - `CLEANUP_CHEAP_PRICE` (default 15, `0` disables cheap-variant deletion)

## Part 5 — What to expect
- eBay API usage per sync drops from ~4+ calls/variant to ~1 call per 25
  variants (+1 inventory-group discovery only on the first sync).
- Amazon request volume drops sharply (parent bulk extraction + fresh-ASIN
  skip + budget limiter) — blocks should become rare; when one happens the
  budget self-heals upward afterward.
- Big listings: all variants now rotate through fresh fetches; between
  rotations the (now functional) 7-day cache keeps them priced.
- Two accounts: fully isolated products/settings/logs/sync state; shared
  Amazon price cache; per-profile extensions that can't cross-sync.
