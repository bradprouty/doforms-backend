import { Redis } from '@upstash/redis';

const redis = Redis.fromEnv();
const DOFORMS_API_BASE = 'https://api.mydoforms.com/api/v2';

function authHeader() {
  const id = process.env.DOFORMS_WEBSERVICE_ID;
  const password = process.env.DOFORMS_PASSWORD;
  return `Bearer ${id}:${password}`;
}

function flattenSubmission(id, detail) {
  const record = { _id: id };
  const fields = (detail && detail.fields) || [];
  for (const f of fields) {
    if (!f || !f.name) continue;
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

  const details = await Promise.all(
    list.map(async (item) => {
      const detailResp = await fetch(
        `${DOFORMS_API_BASE}/submissions/${encodeURIComponent(item.id)}`,
        { headers: { Authorization: authHeader() } }
      );
      if (!detailResp.ok) {
        console.error(`doForms detail failed for ${item.id}: ${detailResp.status}`);
        return null;
      }
      const detail = await detailResp.json();
      return flattenSubmission(item.id, detail);
    })
  );

  return details.filter(Boolean);
}

export default async function handler(req, res) {
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
    const records = await fetchAllSubmissions();
    await redis.set('doforms-data', records);

    console.log('Resync complete. Records stored:', records.length);

    return res.status(200).json({
      success: true,
      message: 'Resync complete',
      recordsStored: records.length,
    });
  } catch (error) {
    console.error('Webhook resync error:', error);
    return res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}
