import { kv } from '@vercel/kv';

export default async function handler(req, res) {
  // Enable CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  if (req.method === 'POST') {
    try {
      // Store the data received from doForms
      const incomingData = req.body;
      
      console.log('Webhook received from doForms:', JSON.stringify(incomingData, null, 2));
      
      // Handle different doForms data formats
      let dataToStore = [];
      
      if (Array.isArray(incomingData)) {
        dataToStore = incomingData;
      } else if (incomingData.data) {
        dataToStore = Array.isArray(incomingData.data) ? incomingData.data : [incomingData.data];
      } else if (incomingData.records) {
        dataToStore = Array.isArray(incomingData.records) ? incomingData.records : [incomingData.records];
      } else {
        dataToStore = [incomingData];
      }
      
      // Store in Vercel KV (persists across requests)
      await kv.set('doforms-data', dataToStore);
      
      console.log('Data stored in KV. Total records:', dataToStore.length);
      
      return res.status(200).json({ 
        success: true, 
        message: 'Webhook received successfully',
        recordsReceived: dataToStore.length 
      });
      
    } catch (error) {
      console.error('Webhook error:', error);
      return res.status(500).json({ 
        success: false, 
        error: error.message 
      });
    }
  }

  res.status(405).json({ error: 'Method not allowed' });
}