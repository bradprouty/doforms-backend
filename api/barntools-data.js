// api/barntools-data.js
//
// Plain GET endpoint. Returns whatever api/barntools-sync.js's last Cron
// run stored in Redis: {byLocation: {location_id: {date: lbs}}, unmapped,
// updatedAt}. foxpro_sync.py (mill computer) polls this the same way it
// polls api/binmaster-data -- no BarnTools credentials or auth needed here,
// same open-read pattern.

import { Redis } from "@upstash/redis";

const redis = Redis.fromEnv();

export default async function handler(req, res) {
  try {
    const stored = await redis.get("barntools-feed-data");
    if (!stored) {
      return res.status(200).json({ byLocation: {}, unmapped: [], updatedAt: null });
    }
    const data = typeof stored === "string" ? JSON.parse(stored) : stored;
    return res.status(200).json(data);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: String(err && err.message ? err.message : err) });
  }
}
