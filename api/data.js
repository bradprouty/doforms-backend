import { Redis } from '@upstash/redis';

const redis = Redis.fromEnv();

export default async function handler(req, res) {
  // Enable CORS so any domain (GitHub Pages, etc.) can read this
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  if (req.method === 'GET') {
    try {
      const raw = await redis.get('doforms-data');

      let data = [];
      if (raw) {
        // Upstash may return a parsed object/array already, or a JSON string
        data = typeof raw === 'string' ? JSON.parse(raw) : raw;
      }

      return res.status(200).json(data);

    } catch (error) {
      console.error('Data fetch error:', error);
      return res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }

  res.status(405).json({ error: 'Method not allowed' });
}
