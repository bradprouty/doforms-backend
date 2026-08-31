// api/barntools-sync.js
//
// Vercel Cron Job target. Runs entirely on Vercel -- no mill computer
// involvement, no Python, same pattern as api/binmaster-sync.js. On its own
// schedule (see vercel.json), this:
//   1. Logs into BarnTools' API (OAuth2 client_credentials grant)
//   2. Pulls the last `days` days of DAILY feed consumption (already computed
//      server-side by BarnTools, already in pounds -- no raw-level diffing
//      or kg->lb conversion needed here, unlike BinMaster) via the
//      `feedTimeSeries` GraphQL query, for the tandem bins and individual
//      bins in BARNTOOLS_LOCATION_MAP below
//   3. Aggregates per FoxPro location (a location can have more than one
//      tandem/sensor id -- e.g. Schaap North has two tandem pairs -- their
//      daily totals are summed)
//   4. MERGES that into whatever daily history is already stored in Redis
//      under "barntools-feed-data", rather than overwriting it -- same
//      "fully covered or fall back" reasoning as BinMaster, see
//      api/binmaster-sync.js's comment for the full explanation
//
// foxpro_sync.py (on the mill computer) reads the result back via a plain
// GET to /api/barntools-data -- it never talks to BarnTools or holds
// BarnTools credentials itself.
//
// Covers (as of 2026-08-31): Schaap North, Raak North, Raak South.
// Schaap South is NOT covered -- Brad doesn't have bin sensors there yet.
//
// BarnTools organizes bins in two ways that both showed up in the real
// schema/data (confirmed 2026-08-31 via GraphQL introspection + live pulls):
//   - "tandem bins": two physical bins BarnTools has already paired and
//     pre-summed into one logical feed unit (Schaap North has two tandem
//     pairs -- confirmed via capacity match: each pair's two bin capacities
//     summed to within 0.01 kg of the tandem's reported capacity. Raak South
//     has one tandem pair covering both its bins).
//   - individual bins (identified by `serialNumber`, queried via
//     `sensorSerials`): used where no tandem pairing exists -- Raak North's
//     two bins are NOT tandem-paired in BarnTools, so they're queried and
//     summed individually here instead.
//
// Location mapping confirmed 2026-08-31 by cross-checking FoxPro's
// farm_bin.dbf BIN_CAPACI against BarnTools' reported bin/tandem capacities
// (same rigor as the BinMaster DW West/East match) -- see project doc Key
// Discovery #14 for the general method. One real find along the way: Raak
// South's FoxPro capacities (8,000 lb) were stale and have been corrected by
// Brad directly in FoxPro to 14,000/12,000 lb, which then matched BarnTools'
// reported capacities almost exactly.
//
// Required Vercel environment variables (Project Settings -> Environment
// Variables -- never commit these):
//   BARNTOOLS_CLIENT_ID
//   BARNTOOLS_CLIENT_SECRET
//   CRON_SECRET          Same shared secret used for api/binmaster-sync.js --
//                        Vercel auto-attaches it as a Bearer token on cron
//                        invocations, and this route checks it too.
//
// Wire the schedule in vercel.json (repo root) alongside the BinMaster cron,
// e.g. 15 minutes offset so they don't both fire at once:
//   { "crons": [
//       { "path": "/api/binmaster-sync", "schedule": "0 6 * * *" },
//       { "path": "/api/barntools-sync", "schedule": "15 6 * * *" }
//   ] }
//
// One-time backfill for phases already in progress when this is deployed,
// same idea as BinMaster's -- call once with a larger window (requires the
// CRON_SECRET Bearer header):
//   GET /api/barntools-sync?days=99
// Unlike BinCloud, there's a hard API ceiling here, not just an unknown
// retention window: BarnTools' feedTimeSeries rejects the WHOLE request if a
// series would return more than 100 daily data points (confirmed 2026-08-31
// via a real validation error requesting 120 days) -- so 99 is the largest
// single backfill this route will ever attempt (MAX_DAYS_BACK below). That's
// still far more than any single FEED_BUDGET phase length, so it's enough to
// fully backfill any phase already in progress.

import { Redis } from "@upstash/redis";

const redis = Redis.fromEnv();

const TOKEN_URL = "https://api.barntools.io/auth/oauth/token";
const GRAPHQL_URL = "https://api.barntools.io/graphql";
const DEFAULT_DAYS_BACK = 10;
// HARD API LIMIT (confirmed 2026-08-31 via a real 401/validation error):
// BarnTools' feedTimeSeries rejects the ENTIRE request -- not a partial
// result -- if a series would return more than 100 data points at DAY1
// granularity. 99 leaves a one-day safety margin for inclusive-range
// off-by-one behavior. This is well beyond any single FEED_BUDGET phase
// length (longest is ~23 days), so it's more than enough to fully backfill
// any phase already in progress -- unlike BinCloud, there's no reason to
// want more than this in one call.
const MAX_DAYS_BACK = 99;

// FoxPro LOCATION_I -> BarnTools tandem-bin ids / individual-bin serial
// numbers. Confirmed 2026-08-31 -- see notes above.
const BARNTOOLS_LOCATION_MAP = {
  // SCHAAP NORTH -- two tandem pairs (East bins combined, West bins combined)
  "11": {
    label: "Schaap North",
    tandemBinIds: [
      "8ed1a28e-3c15-4ef1-8ed9-82c8a0b372a0", // Schaap North West Bins
      "ba9d7582-7904-4507-be46-9941f4d2e471", // Schaap North East Bins
    ],
    sensorSerials: [],
  },
  // RAAK NORTH -- not tandem-paired in BarnTools; two individual bins
  "101": {
    label: "Raak North",
    tandemBinIds: [],
    sensorSerials: [
      "1027012338", // Raak North - Bin South
      "1027015203", // Raak North - Bin North
    ],
  },
  // RAAK SOUTH -- one tandem pair covering both bins
  "102": {
    label: "Raak South",
    tandemBinIds: [
      "77843290-330e-4755-b7af-484e714a5f9c", // Raak South
    ],
    sensorSerials: [],
  },
};

function buildReverseMaps(locationMap) {
  const tandemToLocation = {};
  const sensorToLocation = {};
  for (const [locationId, cfg] of Object.entries(locationMap)) {
    for (const id of cfg.tandemBinIds || []) tandemToLocation[id] = locationId;
    for (const serial of cfg.sensorSerials || []) sensorToLocation[serial] = locationId;
  }
  return { tandemToLocation, sensorToLocation };
}

const { tandemToLocation: TANDEM_ID_TO_LOCATION, sensorToLocation: SENSOR_SERIAL_TO_LOCATION } =
  buildReverseMaps(BARNTOOLS_LOCATION_MAP);

const ALL_TANDEM_IDS = Object.keys(TANDEM_ID_TO_LOCATION);
const ALL_SENSOR_SERIALS = Object.keys(SENSOR_SERIAL_TO_LOCATION);

function dateKey(d) {
  return d.toISOString().slice(0, 10); // YYYY-MM-DD (UTC)
}

async function getAccessToken() {
  const { BARNTOOLS_CLIENT_ID, BARNTOOLS_CLIENT_SECRET } = process.env;
  if (!BARNTOOLS_CLIENT_ID || !BARNTOOLS_CLIENT_SECRET) {
    throw new Error("BARNTOOLS_CLIENT_ID and BARNTOOLS_CLIENT_SECRET must both be set");
  }
  const resp = await fetch(TOKEN_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      client_id: BARNTOOLS_CLIENT_ID,
      client_secret: BARNTOOLS_CLIENT_SECRET,
      grant_type: "client_credentials",
      audience: "api.barntools.io",
    }),
  });
  if (!resp.ok) {
    throw new Error(`BarnTools token request failed: ${resp.status} ${await resp.text()}`);
  }
  const data = await resp.json();
  if (!data.access_token) {
    throw new Error(`BarnTools token response had no access_token: ${JSON.stringify(data)}`);
  }
  return data.access_token;
}

const FEED_HISTORY_QUERY = `
  query FeedHistory(
    $tandemBinIds: [String!]
    $sensorSerials: [String!]
    $start: DateOrDateTimeISO
    $end: DateOrDateTimeISO
    $unit: MassUnit
    $granularity: Granularity
  ) {
    tandems: feedTimeSeries(
      tandemBinIds: $tandemBinIds
      unit: $unit
      granularity: $granularity
      timeRangeV2: { start: $start, end: $end }
    ) {
      id
      consumption { binTs consumption unit }
    }
    singles: feedTimeSeries(
      sensorSerials: $sensorSerials
      unit: $unit
      granularity: $granularity
      timeRangeV2: { start: $start, end: $end }
    ) {
      id
      consumption { binTs consumption unit }
    }
  }
`;

async function fetchFeedHistory(token, startDt, endDt) {
  const resp = await fetch(GRAPHQL_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
    body: JSON.stringify({
      query: FEED_HISTORY_QUERY,
      variables: {
        tandemBinIds: ALL_TANDEM_IDS,
        sensorSerials: ALL_SENSOR_SERIALS,
        start: startDt.toISOString(),
        end: endDt.toISOString(),
        unit: "pounds",
        granularity: "DAY1",
      },
    }),
  });
  if (!resp.ok) {
    throw new Error(`BarnTools feedTimeSeries request failed: ${resp.status} ${await resp.text()}`);
  }
  const body = await resp.json();
  if (body.errors && body.errors.length) {
    throw new Error(`BarnTools feedTimeSeries returned errors: ${JSON.stringify(body.errors)}`);
  }
  return body.data || {};
}

function accumulate(byLocation, locationId, consumptionEntries) {
  if (!byLocation[locationId]) byLocation[locationId] = {};
  for (const entry of consumptionEntries || []) {
    if (entry.binTs == null || entry.consumption == null) continue;
    const day = dateKey(new Date(entry.binTs));
    byLocation[locationId][day] = (byLocation[locationId][day] || 0) + Number(entry.consumption);
  }
}

function aggregateByLocation(tandemsResults, singlesResults) {
  const byLocation = {};
  const unmapped = [];

  for (const series of tandemsResults || []) {
    const locationId = TANDEM_ID_TO_LOCATION[series.id];
    if (!locationId) {
      unmapped.push(`tandem:${series.id}`);
      continue;
    }
    accumulate(byLocation, locationId, series.consumption);
  }
  for (const series of singlesResults || []) {
    const locationId = SENSOR_SERIAL_TO_LOCATION[series.id];
    if (!locationId) {
      unmapped.push(`sensor:${series.id}`);
      continue;
    }
    accumulate(byLocation, locationId, series.consumption);
  }
  return { byLocation, unmapped };
}

// Same merge-not-overwrite logic as api/binmaster-sync.js's mergeByLocation
// -- a day present in both is overwritten by the fresh value (in case of a
// late correction), every older day is kept untouched.
function mergeByLocation(existing, fresh) {
  const merged = {};
  const allLocations = new Set([...Object.keys(existing || {}), ...Object.keys(fresh || {})]);
  for (const loc of allLocations) {
    merged[loc] = { ...(existing && existing[loc]), ...(fresh && fresh[loc]) };
  }
  return merged;
}

export default async function handler(req, res) {
  const { CRON_SECRET } = process.env;
  if (CRON_SECRET) {
    const auth = req.headers["authorization"];
    if (auth !== `Bearer ${CRON_SECRET}`) {
      return res.status(401).json({ error: "unauthorized" });
    }
  }

  const requestedDays = parseInt(req.query && req.query.days, 10);
  const daysBack =
    Number.isFinite(requestedDays) && requestedDays > 0
      ? Math.min(requestedDays, MAX_DAYS_BACK)
      : DEFAULT_DAYS_BACK;

  try {
    const token = await getAccessToken();
    const endDt = new Date();
    const startDt = new Date(endDt.getTime() - daysBack * 24 * 60 * 60 * 1000);

    const { tandems, singles } = await fetchFeedHistory(token, startDt, endDt);
    const { byLocation: freshByLocation, unmapped } = aggregateByLocation(tandems, singles);

    const storedRaw = await redis.get("barntools-feed-data");
    const stored = storedRaw ? (typeof storedRaw === "string" ? JSON.parse(storedRaw) : storedRaw) : null;
    const mergedByLocation = mergeByLocation(stored && stored.byLocation, freshByLocation);

    await redis.set(
      "barntools-feed-data",
      JSON.stringify({ byLocation: mergedByLocation, unmapped, updatedAt: new Date().toISOString() })
    );

    return res.status(200).json({
      ok: true,
      daysFetched: daysBack,
      tandemsRequested: ALL_TANDEM_IDS.length,
      sensorsRequested: ALL_SENSOR_SERIALS.length,
      locations: Object.keys(mergedByLocation).length,
      unmapped,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: String(err && err.message ? err.message : err) });
  }
}
