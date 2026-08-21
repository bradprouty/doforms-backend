import { kv } from '@vercel/kv';

const DOFORMS_BASE = 'https://api.mydoforms.com/api/v2';

function getToken() {
  const id = process.env.DOFORMS_WEBSERVICE_ID;
  const password = process.env.DOFORMS_PASSWORD;
  if (!id || !password) throw new Error('Missing DOFORMS_WEBSERVICE_ID / DOFORMS_PASSWORD env vars');
  return `${id}:${password}`;
}

function flattenSubmission(sub) {
  const row = {};
  for (const f of sub.fields || []) {
    let value = null;
    if (f.text !== undefined) value = f.text;
    else if (f.integer !== undefined) value = f.integer;
    else if (f.number !== undefined) value = f.number;
    else if (f.date !== undefined) value = f.date;
    else if (f.boolean !== undefined) value = f.boolean;
    row[f.name] = value;
  }
  row._id = sub.id;
  row._status = sub.status;
  row._updated = sub.updateTime;
  return row;
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
    console.log('Webhook notification received from doForms:', JSON.stringify(req.body));

    const token = getToken();

    // The notification payload only signals that something changed - its
    // submissionKey isn't a reliably usable identifier on its own. So treat
    // every webhook call as a cue to resync the full submission list.
    const listResp = await fetch(`${DOFORMS_BASE}/submissions`, {
      headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' },
    });

    if (!listResp.ok) {
      const t = await listResp.text();
      throw new Error(`List fetch failed (${listResp.status}): ${t}`);
    }

    const list = await listResp.json(); // [{ key, id }, ...]

    const records = [];
    for (const item of list) {
      const detailResp = await fetch(`${DOFORMS_BASE}/submissions/${encodeURIComponent(item.id)}`, {
        headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' },
      });
      if (!detailResp.ok) continue; // skip any single failure rather than aborting the whole sync
      const detail = await detailResp.json();
      records.push(flattenSubmission(detail));
    }

    await kv.set('doforms-data', JSON.stringify(records));

    return res.status(200).json({ success: true, recordsSynced: records.length });
  } catch (error) {
    console.error('Webhook error:', error);
    return res.status(500).json({ success: false, error: error.message });
  }
}
