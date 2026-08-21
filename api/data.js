import { Redis } from '@upstash/redis';

const redis = Redis.fromEnv();

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const raw = await redis.get('doforms-data');
    let data = [];
    if (raw) data = typeof raw === 'string' ? JSON.parse(raw) : raw;
    return res.status(200).json(data);
  } catch (error) {
    console.error('Data fetch error:', error);
    return res.status(500).json({ error: error.message });
  }
}
