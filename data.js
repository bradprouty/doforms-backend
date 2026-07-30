import { kv } from '@vercel/kv';

export default async function handler(req, res) {
  // Enable CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  if (req.method === 'GET') {
    try {
      // Retrieve data from Vercel KV
      const data = await kv.get('doforms-data');
      
      if (!data) {
        return res.status(200).json([]);
      }
      
      return res.status(200).json(data);
      
    } catch (error) {
      console.error('Error retrieving data:', error);
      return res.status(500).json({ 
        error: 'Failed to retrieve data',
        message: error.message 
      });
    }
  }

  res.status(405).json({ error: 'Method not allowed' });
}