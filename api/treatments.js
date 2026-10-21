// api/treatments.js
// One serverless route for BOTH doForms treatment forms (uses 1 of the 12 Hobby functions).
//
//   GET  /api/treatments                         -> stored records for both forms (dashboard reads this)
//   GET  /api/treatments?form=group              -> just one form (group | individual)
//   POST /api/treatments?form=group&key=SECRET   -> doForms webhook: sync that form
//   GET  /api/treatments?action=sync&form=group&key=SECRET          -> manual sync (repeat until remaining = 0)
//   GET  /api/treatments?action=sync&form=group&key=SECRET&full=1   -> re-fetch every record
//   GET  /api/treatments?action=inspect&form=group&key=SECRET       -> show one record's real field names
//
// Env vars (Vercel -> Settings -> Environment Variables, then redeploy):
//   DOFORMS_GT_WEBSERVICE_ID / DOFORMS_GT_PASSWORD   Group Treatments webservice
//   DOFORMS_IT_WEBSERVICE_ID / DOFORMS_IT_PASSWORD   Individual Treatments webservice
//   TREATMENTS_SYNC_KEY                              any long random string you make up
//   SYNC_TIME_BUDGET_MS  (optional)                  seconds spent fetching details per run, in ms (default 25000)
//   UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN (already set for the Active Groups dashboard)

import { Redis } from '@upstash/redis';

const redis = Redis.fromEnv();
const BASE = 'https://api.mydoforms.com/api/v2';

const FORMS = {
  group: {
    label: 'Group Treatments',
    idEnv: 'DOFORMS_GT_WEBSERVICE_ID',
    pwEnv: 'DOFORMS_GT_PASSWORD',
    key: 'doforms-group-treatments',
  },
  individual: {
    label: 'Individual Treatments',
    idEnv: 'DOFORMS_IT_WEBSERVICE_ID',
    pwEnv: 'DOFORMS_IT_PASSWORD',
    key: 'doforms-individual-treatments',
  },
};

const PAGE_SIZE = 1000;              // doForms max per list call
const CONCURRENCY = 2;               // keep low: the rate limit is undocumented
const TIME_BUDGET_MS = Number(process.env.SYNC_TIME_BUDGET_MS) || 25000; // detail-fetch time per run (listing comes first and isn't counted)
const LOCK_MS = 90000;               // one sync per form at a time
const COOLDOWN_MS = 15 * 60 * 1000;  // after any 429, don't call doForms for 15 min
const COOLDOWN_KEY = 'doforms-treatments:cooldown';
const CHANGE_OVERLAP_MS = 10 * 60 * 1000;

// ---------- doForms helpers ----------

function tokenFor(form) {
  const id = process.env[form.idEnv];
  const pw = process.env[form.pwEnv];
  if (!id || !pw) {
    const e = new Error(`Missing env vars ${form.idEnv} / ${form.pwEnv}`);
    e.status = 500;
    throw e;
  }
  return `${id}:${pw}`; // plain, NOT base64
}

function df(path, token) {
  return fetch(BASE + path, { headers: { Authorization: `Bearer ${token}` } });
}

// Lists every submission id, 1000 at a time. `extra` adds query filters.
async function listIds(token, extra = '') {
  const ids = [];
  const seen = new Set();
  for (let skip = 0; skip <= 500000; skip += PAGE_SIZE) {
    const r = await df(`/submissions?limit=${PAGE_SIZE}&skip=${skip}${extra}`, token);
    if (!r.ok) {
      const e = new Error(`list failed: ${r.status}`);
      e.status = r.status;
      throw e;
    }
    const body = await r.json();
    const page = Array.isArray(body) ? body : (body.submissions || body.items || []);
    let added = 0;
    for (const s of page) {
      if (s && s.id && !seen.has(s.id)) { seen.add(s.id); ids.push(s.id); added++; }
    }
    if (page.length < PAGE_SIZE || added === 0) break; // last page (or paging ignored)
  }
  return ids;
}

// A field's `data` names the property holding the value: field[field.data]
function flattenFields(fields) {
  const rec = {};
  for (const f of fields || []) {
    if (!f || !f.name) continue;
    if (['label', 'action', 'instruction', 'page', 'button', 'email', 'fax'].includes(f.type)) continue; // email = addresses, keep out of public data
    let v = f.data ? f[f.data] : undefined;
    if (f.data === 'time' && typeof v === 'string') {
      const m = v.match(/T(\d{2}:\d{2})/);                     // "1970-01-01T15:39:00.000-05:00" -> "15:39"
      rec[f.name] = m ? m[1] : v;
      continue;
    }
    if (f.data === 'rows' && Array.isArray(v)) {
      v = v.map(row => flattenFields(row.fields));            // table / repeat -> array of objects
    } else if (f.data === 'fields' && Array.isArray(v)) {
      Object.assign(rec, flattenFields(v));                   // grid -> lift child fields
      continue;
    } else if (f.data === 'strings' && Array.isArray(v)) {
      v = v.join(', ');                                       // choose multiple
    } else if (f.data === 'blob') {
      v = v ? '[file]' : null;                                // photos / signatures
    } else if (f.data === 'location' && v && typeof v === 'object') {
      v = v.latitude != null ? `${v.latitude}, ${v.longitude}` : null;
    }
    rec[f.name] = v === undefined ? null : v;
  }
  return rec;
}

function flattenSubmission(sub) {
  return {
    _id: sub.id,
    _createTime: sub.createTime || null,
    _receiveTime: sub.receiveTime || null,
    _updateTime: sub.updateTime || null,
    _enteredBy: sub.sourceName || sub.ownerName || null,
    ...flattenFields(sub.fields),
  };
}

// Fetches details with low concurrency. Stops at the first 429 (no retries:
// retries seemed to extend doForms' cooldown) or when the time budget runs out.
// Saves to Redis every 10 records, so progress survives a timeout.
async function fetchDetails(token, ids, started, hashKey) {
  let i = 0, fetched = 0, failed = 0, stopped = null, rateLimited = false;
  const failures = [];
  let buf = {}, bufN = 0;

  async function flush() {
    if (!bufN) return;
    const batch = buf; buf = {}; bufN = 0;
    await redis.hset(hashKey, batch);
  }

  async function worker() {
    while (!stopped && i < ids.length) {
      if (Date.now() - started > TIME_BUDGET_MS) { stopped = 'time budget reached'; break; }
      const id = ids[i++];
      const r = await df(`/submissions/${id}`, token);
      if (r.ok) {
        const rec = flattenSubmission(await r.json()); // await BEFORE touching buf (another worker may flush it)
        buf[id] = rec;
        bufN++; fetched++;
        if (bufN >= 10) await flush();
      } else if (r.status === 429) {
        stopped = 'rate limited (429)'; rateLimited = true;
      } else {
        failed++;
        if (failures.length < 10) failures.push({ id, status: r.status });
      }
    }
  }

  await Promise.all(Array.from({ length: CONCURRENCY }, worker));
  await flush();
  return { fetched, failedDetailRequests: failed, failures, stopped, rateLimited };
}

// ---------- sync ----------

async function syncForm(name, opts) {
  const form = FORMS[name];
  const metaKey = form.key + ':meta';
  const lockKey = form.key + ':lock';
  const started = Date.now();
  const result = { form: name, startedAt: new Date(started).toISOString() };

  const cooldown = await redis.pttl(COOLDOWN_KEY);
  if (cooldown > 0) {
    return { ...result, skipped: `doForms rate-limit cooldown, ${Math.ceil(cooldown / 60000)} min left` };
  }

  const gotLock = await redis.set(lockKey, started, { nx: true, px: LOCK_MS });
  if (!gotLock) return { ...result, skipped: 'another sync for this form is running' };

  let meta = {};
  try {
    meta = (await redis.get(metaKey)) || {};
    const token = tokenFor(form);

    const ids = await listIds(token);
    const idSet = new Set(ids);
    const storedIds = (await redis.hkeys(form.key)) || [];
    const storedSet = new Set(storedIds);
    result.listed = ids.length;
    result.previousCount = storedIds.length;

    // Guard: never wipe good data because doForms returned nothing.
    if (ids.length === 0 && storedIds.length > 0) {
      result.error = 'doForms listed 0 submissions; refusing to wipe stored data';
      result.status = 502;
      return result;
    }

    // Records deleted in doForms
    const removed = storedIds.filter(id => !idSet.has(id));
    if (removed.length) {
      const suspicious = removed.length > Math.max(5, storedIds.length * 0.5);
      if (suspicious && !opts.force) {
        result.removalSkipped = `${removed.length} records missing from doForms list; not deleting (add &force=1 if intended)`;
      } else {
        for (let k = 0; k < removed.length; k += 500) await redis.hdel(form.key, ...removed.slice(k, k + 500));
        result.removed = removed.length;
      }
    }

    // What needs fetching: new ids + ids edited since the last good sync
    let toFetch;
    if (opts.full) {
      toFetch = ids;
    } else {
      const want = new Set(ids.filter(id => !storedSet.has(id)));
      if (meta.lastSync) {
        const begin = new Date(Date.parse(meta.lastSync) - CHANGE_OVERLAP_MS).toISOString();
        const end = new Date(started + 24 * 3600 * 1000).toISOString();
        try {
          const changed = await listIds(token, `&updateTimeBegin=${encodeURIComponent(begin)}&updateTimeEnd=${encodeURIComponent(end)}`);
          for (const id of changed) if (idSet.has(id)) want.add(id);
          result.editedSinceLastSync = changed.length;
        } catch (e) {
          if (e.status === 429) throw e;
          result.editedQuery = `not supported by doForms (${e.status || e.message}); use &full=1 occasionally to pick up edits`;
        }
      }
      toFetch = [...want];
    }
    result.toFetch = toFetch.length;

    const fr = await fetchDetails(token, toFetch, Date.now(), form.key); // budget starts after listing
    Object.assign(result, fr);
    result.remaining = toFetch.length - fr.fetched;

    if (fr.rateLimited) {
      await redis.set(COOLDOWN_KEY, 1, { px: COOLDOWN_MS });
      result.status = 429;
    }
    if (result.remaining === 0) meta.lastSync = result.startedAt;
    return result;
  } catch (e) {
    if (e.status === 429) {
      await redis.set(COOLDOWN_KEY, 1, { px: COOLDOWN_MS });
      result.rateLimited = true;
    }
    result.error = e.message;
    result.status = e.status === 429 ? 429 : (e.status === 500 ? 500 : 502);
    return result;
  } finally {
    result.count = await redis.hlen(form.key);
    result.ms = Date.now() - started;
    meta.lastAttempt = result.startedAt;
    meta.lastResult = { ...result, failures: undefined };
    await redis.set(metaKey, meta);
    await redis.del(lockKey);
  }
}

// ---------- inspect (find the real field names) ----------

async function inspectForm(name) {
  const form = FORMS[name];
  const token = tokenFor(form);
  const r = await df('/submissions?limit=1', token);
  if (!r.ok) return { form: name, error: `list failed: ${r.status}` };
  const list = await r.json();
  const first = (Array.isArray(list) ? list : list.submissions || [])[0];
  if (!first) return { form: name, note: 'no submissions yet' };
  const d = await df(`/submissions/${first.id}`, token);
  if (!d.ok) return { form: name, listItem: first, error: `detail failed: ${d.status}` };
  const sub = await d.json();
  return {
    form: name,
    listItem: first,
    fields: (sub.fields || []).map(f => ({ name: f.name, type: f.type, data: f.data, value: f.data ? f[f.data] : undefined })),
    flattened: flattenSubmission(sub),
  };
}

// ---------- handler ----------

function pickForms(form) {
  if (form && FORMS[form]) return [form];
  return Object.keys(FORMS);
}

export default async function handler(req, res) {
  res.setHeader('Cache-Control', 'no-store');
  const q = req.query || {};
  const action = q.action || (req.method === 'POST' ? 'sync' : 'data');

  if (action === 'data') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    const out = { generatedAt: new Date().toISOString(), forms: {} };
    for (const name of pickForms(q.form)) {
      const f = FORMS[name];
      const [recs, meta] = await Promise.all([redis.hgetall(f.key), redis.get(f.key + ':meta')]);
      out.forms[name] = {
        label: f.label,
        lastSync: meta?.lastSync || null,
        lastAttempt: meta?.lastAttempt || null,
        lastResult: meta?.lastResult || null,
        records: recs ? Object.values(recs) : [],
      };
    }
    return res.status(200).json(out);
  }

  // Everything below talks to doForms, so it needs the secret key.
  if (!process.env.TREATMENTS_SYNC_KEY || q.key !== process.env.TREATMENTS_SYNC_KEY) {
    return res.status(401).json({ error: 'missing or wrong key' });
  }

  try {
    if (action === 'inspect') {
      if (!FORMS[q.form]) return res.status(400).json({ error: 'add &form=group or &form=individual' });
      return res.status(200).json(await inspectForm(q.form));
    }

    if (action === 'sync') {
      const opts = { full: q.full === '1', force: q.force === '1' };
      const results = [];
      for (const name of pickForms(q.form)) results.push(await syncForm(name, opts));
      // Webhook calls always get 200 so doForms doesn't retry (each retry = more API calls).
      const worst = Math.max(200, ...results.map(r => r.status || 200));
      return res.status(req.method === 'POST' ? 200 : worst).json({ results });
    }

    return res.status(400).json({ error: `unknown action ${action}` });
  } catch (e) {
    return res.status(e.status || 500).json({ error: e.message });
  }
}
