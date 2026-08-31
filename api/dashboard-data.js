// api/dashboard-data.js
//
// Merges the doForms group data ("doforms-data", populated by webhook.js)
// with the FoxPro-derived feed progress data ("foxpro-feed-data",
// populated by foxpro_sync.py via api/feed-data.js), matched by GroupID
// (doForms) <-> AUX_ID (FoxPro), disambiguated by Location when a single
// AUX_ID covers more than one physical location (e.g. a group split
// across two barns).
//
// This is what dashboard.html should poll instead of api/data.js directly,
// so it gets both the doForms inventory fields and the feed-progress
// columns in one response.

import { Redis } from '@upstash/redis';

const redis = Redis.fromEnv();

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const [doformsData, feedData] = await Promise.all([
      redis.get('doforms-data'),
      redis.get('foxpro-feed-data'),
    ]);

    const doformsRecords = Array.isArray(doformsData) ? doformsData : [];
    const feedRecords = feedData && Array.isArray(feedData.groups) ? feedData.groups : [];

    // Index feed records by (aux_id, location) and by aux_id alone.
    const byAuxLoc = new Map();
    const byAux = new Map();
    for (const f of feedRecords) {
      const auxId = (f.aux_id || '').trim();
      const loc = String(f.location || '').trim();
      byAuxLoc.set(`${auxId}::${loc}`, f);
      if (!byAux.has(auxId)) byAux.set(auxId, []);
      byAux.get(auxId).push(f);
    }

    const matchedFeedKeys = new Set();

    const merged = doformsRecords.map((d) => {
      const groupId = (d.GroupID || '').trim();
      const loc = String(d.Location || '').trim();

      let feed = byAuxLoc.get(`${groupId}::${loc}`);
      if (!feed) {
        const candidates = byAux.get(groupId) || [];
        if (candidates.length === 1) feed = candidates[0];
      }

      if (feed) {
        matchedFeedKeys.add(`${(feed.aux_id || '').trim()}::${String(feed.location || '').trim()}`);
      }

      return {
        ...d,
        feed: feed
          ? {
              phase: feed.phase,
              head_count: feed.head_count,
              total_lbs_fed: feed.total_lbs_fed,
              lbs_feed_per_head: feed.lbs_feed_per_head,
              pct_of_total_budget: feed.pct_of_total_budget,
              predicted_weight: feed.predicted_weight,
              bin_scale_phases: feed.bin_scale_phases,
            }
          : null,
      };
    });

    // FoxPro groups with no doForms match at all (e.g. brand-new placements
    // doForms hasn't recorded yet) -- surface them too rather than silently
    // dropping them.
    const unmatchedFeed = feedRecords.filter(
      (f) => !matchedFeedKeys.has(`${(f.aux_id || '').trim()}::${String(f.location || '').trim()}`)
    );

    return res.status(200).json({
      generated_at: feedData ? feedData.generated_at : null,
      groups: merged,
      unmatched_feed_groups: unmatchedFeed,
    });
  } catch (err) {
    return res.status(500).json({ error: 'Failed to build dashboard data', details: String(err) });
  }
}
