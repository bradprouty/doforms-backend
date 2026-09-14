import { Redis } from '@upstash/redis';

const redis = Redis.fromEnv();
const DOFORMS_API_BASE = 'https://api.mydoforms.com/api/v2';

function authHeader() {
  const id = process.env.DOFORMS_WEBSERVICE_ID;
  const password = process.env.DOFORMS_PASSWORD;
  return `Bearer ${id}:${password}`;
}

// doForms's webhook POST body is just a change-notification envelope with
// no real field data -- it's used here only as a trigger. The actual data
// comes from doForms's own REST API: list every submission, then fetch each
// one's full field data and flatten it into a plain object.
function flattenSubmission(id, detail) {
  const record = { _id: id };
  const fields = (detail && detail.fields) || [];
  for (const f of fields) {
    if (!f || !f.name) continue;
    // Each field object looks like { name, data, type, <typed value> } --
    // "data" is NOT the value itself, it's the NAME of the property that
    // holds the value (e.g. data: "text" means the real value is in f.text;
    // data: "integer" means it's in f.integer). Confirmed against a real
    // submission on 2026-09-11:
    //   { name: "GroupID", data: "text", type: "text", text: "2634NN2EN" }
    //   { name: "Died", data: "integer", type: "numeric", integer: 31 }
    const valueKey = f.data;
    record[f.name] =
      valueKey && Object.prototype.hasOwnProperty.call(f, valueKey) ? f[valueKey] : null;
  }
  return record;
}

async function fetchAllSubmissions() {
  const listResp = await fetch(`${DOFORMS_API_BASE}/submissions`, {
    headers: { Authorization: authHeader() },
  });
  if (!listResp.ok) {
    throw new Error(`doForms list failed: ${listResp.status} ${await listResp.text()}`);
  }
  const list = await listResp.json();

  let failedCount = 0;
  const details = await Promise.all(
    list.map(async (item) => {
      const detailResp = await fetch(
        `${DOFORMS_API_BASE}/submissions/${encodeURIComponent(item.id)}`,
        { headers: { Authorization: authHeader() } }
      );
      if (!detailResp.ok) {
        console.error(`doForms detail failed for ${item.id}: ${detailResp.status}`);
        failedCount++;
        return null;
      }
      const detail = await detailResp.json();
      return flattenSubmission(item.id, detail);
    })
  );

  return { records: details.filter(Boolean), attempted: list.length, failedCount };
}

export default async function handler(req, res) {
  // Enable CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const { records, attempted, failedCount } = await fetchAllSubmissions();

    const existingRaw = await redis.get('doforms-data');
    const previousCount = Array.isArray(existingRaw) ? existingRaw.length : 0;

    // Safety guard: never let a doForms-side hiccup blank out real stored
    // data. Incident 2026-09-14: doForms's API briefly rejected every
    // per-record detail request while the initial list call still
    // succeeded -- each failure was caught and skipped (not thrown), so
    // fetchAllSubmissions quietly returned an empty array and this route
    // happily overwrote 40 good records with []. A resync that comes back
    // empty, or with far fewer records than what's already stored, almost
    // certainly means doForms had a problem answering -- not that every
    // active group vanished between one resync and the next. When that
    // looks like what's happening, keep the existing data untouched and
    // report the situation instead of silently "succeeding."
    const suspiciouslyLow =
      previousCount > 0 && (records.length === 0 || records.length < previousCount * 0.5);

    if (suspiciouslyLow) {
      console.error(
        `Refusing to overwrite doforms-data: fetched ${records.length} record(s) ` +
          `(${failedCount} of ${attempted} detail requests failed), but ${previousCount} ` +
          `were already stored. Keeping the existing data untouched.`
      );
      return res.status(502).json({
        success: false,
        error:
          'doForms resync looked incomplete (too few records came back) -- kept the existing data instead of overwriting it',
        fetched: records.length,
        attempted,
        failedDetailRequests: failedCount,
        previousCount,
      });
    }

    await redis.set('doforms-data', records);

    console.log('Resync complete. Records stored:', records.length);

    return res.status(200).json({
      success: true,
      message: 'Resync complete',
      recordsStored: records.length,
      failedDetailRequests: failedCount,
    });
  } catch (error) {
    console.error('Webhook resync error:', error);
    return res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}
