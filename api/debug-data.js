import { Redis } from '@upstash/redis';

const redis = Redis.fromEnv();

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  try {
    const raw = await redis.get('doforms-data');
    let parsed = null;
    let parseError = null;

    if (raw) {
      if (typeof raw === 'string') {
        try {
          parsed = JSON.parse(raw);
        } catch (e) {
          parseError = e.message;
        }
      } else {
        parsed = raw;
      }
    }

    return res.status(200).json({
      rawType: typeof raw,
      rawPreview: typeof raw === 'string' ? raw.slice(0, 500) : raw,
      parseError,
      parsedIsArray: Array.isArray(parsed),
      parsedLength: Array.isArray(parsed) ? parsed.length : null,
    });
  } catch (error) {
    return res.status(200).json({
      caughtError: error.message,
      stack: error.stack,
    });
  }
}
