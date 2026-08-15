// descrefresh.js — repair listing descriptions IN PLACE, without re-pushing
//
// WHY NOT A RE-PUSH
// A bulk re-push recreates SKUs from hashed variant values, so any value that
// changed since the original push produces a NEW SKU — you get duplicate
// variants instead of repaired ones. It also sweeps up VeRO-removed listings
// and burns the revision budget across the whole catalogue.
//
// WHAT THIS DOES INSTEAD
// For each listing: GET the existing inventory item group, run the description
// through the variant-neutraliser, and PUT the group back with ONLY the
// description changed. variantSKUs, aspects, images, and title are passed
// through untouched, so no variant is created, removed, or re-keyed.
//
// HONEST CAVEAT ABOUT PUBLISH
// Updating the group stores the new description, but eBay propagates it to the
// live listing on the next publish of that group. Publishing an EXISTING group
// is a revision, not a relist — item ID, watchers, and sales history are kept.
// It still counts against the 250-revisions-per-listing-per-day cap and is
// still account activity, so publish is OFF by default: descriptions are
// staged now and go live on the listing's next ordinary sync.
//
// SAFETY
//   • Dry-run by default — nothing is written until you pass apply:true.
//   • Skips anything flagged do_not_relist (VeRO).
//   • Skips listings whose description the neutraliser wouldn't change.
//   • Batched and throttled; never touches variantSKUs.

const { neutralizeDescription } = require('./neutralize');

let _pool = null;
function initDescRefresh(pool) {
  _pool = pool;
  console.log('[desc] in-place description refresh ready');
}

function _auth(token) {
  return {
    'Authorization': `Bearer ${token}`,
    'Content-Type': 'application/json',
    'Accept-Language': 'en-US',
    'Content-Language': 'en-US',
  };
}

/**
 * Preview (or apply) the neutralised description for one inventory item group.
 */
async function refreshOne({ token, apiBase, groupSku, variations, variantCount,
                            apply = false, publish = false }) {
  const auth = _auth(token);
  const url = `${apiBase}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`;
  const gr = await fetch(url, { headers: auth });
  if (!gr.ok) {
    const t = await gr.text().catch(() => '');
    return { groupSku, status: 'error', error: `GET group ${gr.status}: ${t.slice(0, 160)}` };
  }
  const group = await gr.json();
  const before = group.description || '';
  if (!before) return { groupSku, status: 'skipped', reason: 'no description on group' };

  const r = neutralizeDescription(before, {
    variations: variations || null,
    variantCount: variantCount || (group.variantSKUs || []).length,
  });
  if (!r.changed || r.description === before) {
    return { groupSku, status: 'unchanged', removed: [] };
  }
  if (!apply) {
    return {
      groupSku, status: 'preview',
      removed: r.removed,
      beforeLen: before.length, afterLen: r.description.length,
      beforeSnippet: before.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 240),
      afterSnippet: r.description.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 240),
      variantCount: (group.variantSKUs || []).length,
    };
  }

  // Write back: description ONLY. Everything else is echoed unchanged so no
  // variant is added, dropped, or re-keyed by this operation.
  const body = { ...group, description: r.description };
  delete body.inventoryItemGroupKey; // eBay rejects it in the PUT body
  const pr = await fetch(url, { method: 'PUT', headers: auth, body: JSON.stringify(body) });
  if (!pr.ok) {
    const t = await pr.text().catch(() => '');
    return { groupSku, status: 'error', error: `PUT group ${pr.status}: ${t.slice(0, 200)}`, removed: r.removed };
  }

  let published = false;
  if (publish) {
    const pubR = await fetch(`${apiBase}/sell/inventory/v1/offer/publish_by_inventory_item_group`, {
      method: 'POST', headers: auth,
      body: JSON.stringify({ inventoryItemGroupKey: groupSku, marketplaceId: 'EBAY_US' }),
    });
    published = pubR.ok;
    if (!pubR.ok) {
      const t = await pubR.text().catch(() => '');
      console.warn(`[desc] ${groupSku} publish ${pubR.status}: ${t.slice(0, 160)}`);
    }
  }
  return { groupSku, status: 'applied', published, removed: r.removed,
           beforeLen: before.length, afterLen: r.description.length };
}

async function _listings(account, limit, offset) {
  const r = await _pool.query(
    `SELECT id, ebay_sku, title, data, do_not_relist
       FROM products
      WHERE account_id = $1 AND status = 'listed' AND ebay_sku IS NOT NULL
        AND COALESCE(do_not_relist, FALSE) = FALSE
      ORDER BY updated_at DESC
      LIMIT $2 OFFSET $3`, [account, limit, offset]);
  return r.rows;
}

function mountDescRefresh(app, getEbayUrls) {
  const acct = req => {
    const a = String(req.query.account || '').trim();
    return /^[\w.\-]{1,64}$/.test(a) ? a : 'default';
  };

  app.post('/api/desc/refresh', async (req, res) => {
    try {
      const { access_token, apply = false, publish = false,
              limit = 20, offset = 0, skus } = req.body || {};
      if (!access_token) return res.status(400).json({ error: 'access_token required' });
      if (!_pool) return res.status(500).json({ error: 'DB not ready' });
      const apiBase = (getEbayUrls().EBAY_API) || 'https://api.ebay.com';
      const account = acct(req);

      let rows;
      if (Array.isArray(skus) && skus.length) {
        const r = await _pool.query(
          `SELECT id, ebay_sku, title, data, do_not_relist FROM products
            WHERE account_id = $1 AND ebay_sku = ANY($2)`, [account, skus]);
        rows = r.rows.filter(x => !x.do_not_relist);
      } else {
        rows = await _listings(account, Math.min(100, parseInt(limit) || 20), parseInt(offset) || 0);
      }

      const results = [];
      for (const row of rows) {
        const d = row.data || {};
        const variations = d.variationValues ||
          (d.comboAsin ? Object.keys(d.comboAsin) : null);
        const out = await refreshOne({
          token: access_token, apiBase,
          groupSku: row.ebay_sku,
          variations,
          variantCount: d.comboAsin ? Object.keys(d.comboAsin).length : 0,
          apply: !!apply, publish: !!publish,
        });
        out.title = (row.title || '').slice(0, 80);
        results.push(out);
        await new Promise(r2 => setTimeout(r2, 250)); // gentle on the API
      }
      const counts = results.reduce((a, r2) => { a[r2.status] = (a[r2.status] || 0) + 1; return a; }, {});
      console.log(`[desc] ${apply ? 'APPLIED' : 'preview'} ${rows.length} listings: ${JSON.stringify(counts)}`);
      res.json({ success: true, applied: !!apply, published: !!publish, counts, results });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/desc-refresh', (req, res) => {
    res.setHeader('Content-Type', 'text/html');
    res.send(PAGE);
  });

  console.log('[desc] routes mounted: /desc-refresh, /api/desc/refresh');
}

const PAGE = `<!DOCTYPE html><html><head><meta charset="utf-8"><title>DropSync — description refresh</title><style>
*{box-sizing:border-box}body{font-family:system-ui,sans-serif;background:#0f1116;color:#e8eaf0;margin:0;padding:22px}
h1{font-size:19px;margin:0 0 4px} .sub{opacity:.65;font-size:13px;margin-bottom:16px;max-width:820px;line-height:1.5}
button{background:#2b5cd9;border:0;color:#fff;padding:8px 14px;border-radius:7px;cursor:pointer;font-size:13px;margin-right:8px}
button.danger{background:#c92a2a} button:disabled{opacity:.5;cursor:default}
input,label{font-size:13px} input[type=number]{width:70px;background:#1a1e27;border:1px solid #2a3040;color:#e6e8ee;border-radius:6px;padding:6px}
#status{margin:12px 0;font-size:13px;min-height:18px}
.card{background:#161922;border:1px solid #232836;border-radius:9px;padding:12px;margin-bottom:10px}
.t{font-weight:600;font-size:13px;margin-bottom:6px}
.rm{color:#ff9b9b;font-size:12px;margin:2px 0 2px 14px}
.ba{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:8px;font-size:12px}
.ba div{background:#10131a;border:1px solid #202533;border-radius:6px;padding:8px;max-height:130px;overflow:auto}
.lbl{opacity:.5;font-size:10px;text-transform:uppercase;margin-bottom:3px}
.warn{background:#3a2a12;border:1px solid #6b4a19;padding:10px;border-radius:8px;margin-bottom:14px;font-size:13px;line-height:1.5}
</style></head><body>
<h1>Description refresh (in place)</h1>
<div class="sub">Removes variant-specific claims ("100 Pack", "Colour: Black") that misdescribe the other variants, plus Amazon artefacts. Updates the inventory group description only — no variant is created, removed, or re-keyed, and no listing is relisted.</div>
<div class="warn"><b>Publish is off by default.</b> Changes are staged on the group and go live on the listing's next ordinary sync. Ticking publish revises the listing immediately (keeps item ID and history) but counts as account activity — leave it off while your account is under any restriction.</div>
<div>
  <label>Batch <input type="number" id="limit" value="20" min="1" max="100"></label>
  <label>Offset <input type="number" id="offset" value="0" min="0"></label>
  <button onclick="run(false,false)">Preview</button>
  <button onclick="run(true,false)" class="danger">Apply (stage only)</button>
  <label style="margin-left:10px"><input type="checkbox" id="pub"> also publish now</label>
</div>
<div id="status"></div><div id="out"></div>
<script>
const S=JSON.parse(localStorage.getItem('ds_settings')||'{}');
function q(){return S.accountId?'?account='+encodeURIComponent(S.accountId):''}
async function run(apply){
  if(!S.token){alert('No eBay token in this browser — open DropSync and connect eBay first.');return}
  const publish=document.getElementById('pub').checked;
  if(apply&&!confirm('Apply to this batch?'+(publish?'\\n\\nPUBLISH IS ON — listings will be revised immediately.':'\\n\\nStaging only; no listing is revised now.'))) return;
  document.getElementById('status').textContent=apply?'Applying…':'Previewing…';
  const r=await fetch('/api/desc/refresh'+q(),{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({access_token:S.token,apply,publish,
      limit:+document.getElementById('limit').value,offset:+document.getElementById('offset').value})});
  const d=await r.json();
  if(d.error){document.getElementById('status').textContent='Error: '+d.error;return}
  document.getElementById('status').textContent=(apply?'Applied. ':'Preview. ')+JSON.stringify(d.counts);
  document.getElementById('out').innerHTML=d.results.map(x=>{
    if(x.status==='unchanged'||x.status==='skipped')return '';
    return '<div class="card"><div class="t">'+(x.title||x.groupSku)+' <span style="opacity:.5">('+x.status+
      (x.published?', published':'')+')</span></div>'+
      (x.error?'<div class="rm">'+x.error+'</div>':'')+
      (x.removed||[]).map(z=>'<div class="rm">✗ '+z.replace(/[<>&]/g,c=>({'<':'&lt;','>':'&gt;','&':'&amp;'}[c]))+'</div>').join('')+
      (x.beforeSnippet?'<div class="ba"><div><div class="lbl">before</div>'+x.beforeSnippet+'</div><div><div class="lbl">after</div>'+x.afterSnippet+'</div></div>':'')+
      '</div>';
  }).join('')||'<div class="sub">Nothing needed changing in this batch.</div>';
}
</script></body></html>`;

module.exports = { initDescRefresh, mountDescRefresh, refreshOne };
