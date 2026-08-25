// api/feed-data.js
//
// Receives the computed FoxPro feed-progress payload from foxpro_sync.py
// (run on a machine with real access to the EasyFeed database) and stores
// it in Redis under "foxpro-feed-data". GET returns the stored payload
// as-is (useful for debugging / spot-checking what the sync script sent).
//
// POST requests must include header:  x-feed-secret: <FEED_SYNC_SECRET>
// (set FEED_SYNC_SECRET as a Vercel environment variable -- pick any long
// random string and use the same value in the sync script's config).

import { Redis } from '@upstash/redis';

const redis = Redis.fromEnv();

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-feed-secret');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  if (req.method === 'POST') {
    const secret = req.headers['x-feed-secret'];
    if (!process.env.FEED_SYNC_SECRET || secret !== process.env.FEED_SYNC_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const body = req.body;
    if (!body || !Array.isArray(body.groups)) {
      return res.status(400).json({ error: 'Expected { groups: [...] }' });
    }

    try {
      await redis.set('foxpro-feed-data', body);
      return res.status(200).json({ ok: true, count: body.groups.length });
    } catch (err) {
      return res.status(500).json({ error: 'Failed to store feed data', details: String(err) });
    }
  }

  if (req.method === 'GET') {
    try {
      const data = await redis.get('foxpro-feed-data');
      return res.status(200).json(data || { generated_at: null, groups: [] });
    } catch (err) {
      return res.status(500).json({ error: 'Failed to read feed data', details: String(err) });
    }
  }

  return res.status(405).json({ error: 'Method not allowed' });
}
