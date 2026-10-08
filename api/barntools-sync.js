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
// Added 2026-10-08 (feed logistics dashboard): besides daily consumption this
// route now ALSO stores, in the same Redis record:
//   levelByLocation  { loc: { lbs, capacity, asOf, bins: [{label, lbs, t}] } }
//                    the most recent bin level per location (summed over its
//                    tandems/bins). Replaced every run.
//   deliveryEvents   { loc: [{ t, lbs, bin }] }
//                    feed deliveries detected as sustained level RISES in the
//                    hourly level history, same rule as api/binmaster-sync.js.
//   eventsFrom       earliest time the stored events can be trusted from.
// Source: the same feedTimeSeries query, asked at HOUR1 granularity for the
// `current` (pounds in the bin) field of each consumption bucket. The 100-point
// ceiling mentioned below means one run covers about 4 days of hourly levels.
// Failures in this extra step never break the daily consumption sync; the
// error text is stored under `levelError`.
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

// ---------------------------------------------------------------------------
// Bin levels + delivery detection (added 2026-10-08)
// ---------------------------------------------------------------------------
// HOUR1 granularity under the 100-data-point ceiling => 95 hours per run.
const LEVEL_HOURS = 95;
// A delivery is a stretch of rising hourly levels (small dips tolerated). Same
// idea and defaults as api/binmaster-sync.js; hourly points are coarser, so the
// allowed gap between points is a little looser.
const DELIVERY_MIN_LBS = 1500;
const DELIVERY_DIP_TOL = 400;
const DELIVERY_MAX_GAP_MS = 3 * 60 * 60 * 1000;
const EVENT_RETENTION_DAYS = 75;
const EVENT_EDGE_MS = 6 * 60 * 60 * 1000;

const LEVEL_HISTORY_QUERY = `
  query LevelHistory(
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
      consumption { binTs lastValidReadingTs current capacity unit }
    }
    singles: feedTimeSeries(
      sensorSerials: $sensorSerials
      unit: $unit
      granularity: $granularity
      timeRangeV2: { start: $start, end: $end }
    ) {
      id
      consumption { binTs lastValidReadingTs current capacity unit }
    }
  }
`;

async function fetchLevelHistory(token, endDt) {
  const startDt = new Date(endDt.getTime() - LEVEL_HOURS * 60 * 60 * 1000);
  const resp = await fetch(GRAPHQL_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
    body: JSON.stringify({
      query: LEVEL_HISTORY_QUERY,
      variables: {
        tandemBinIds: ALL_TANDEM_IDS,
        sensorSerials: ALL_SENSOR_SERIALS,
        start: startDt.toISOString(),
        end: endDt.toISOString(),
        unit: "pounds",
        granularity: "HOUR1",
      },
    }),
  });
  if (!resp.ok) {
    throw new Error(`BarnTools level-history request failed: ${resp.status} ${await resp.text()}`);
  }
  const body = await resp.json();
  if (body.errors && body.errors.length) {
    throw new Error(`BarnTools level-history returned errors: ${JSON.stringify(body.errors).slice(0, 400)}`);
  }
  return { data: body.data || {}, startDt };
}

function detectDeliveries(readings) {
  const events = [];
  let run = null;
  const close = () => {
    if (run && run.peak - run.startMass >= DELIVERY_MIN_LBS) {
      events.push({ t: run.peakT.toISOString(), lbs: Math.round(run.peak - run.startMass) });
    }
    run = null;
  };
  for (let i = 1; i < readings.length; i++) {
    const [t0, m0] = readings[i - 1];
    const [t1, m1] = readings[i];
    if (t1 - t0 > DELIVERY_MAX_GAP_MS) {
      close();
      continue;
    }
    if (!run) {
      if (m1 > m0) run = { startMass: m0, peak: m1, peakT: t1 };
      continue;
    }
    if (m1 > run.peak) {
      run.peak = m1;
      run.peakT = t1;
    } else if (run.peak - m1 > DELIVERY_DIP_TOL) {
      close();
    }
  }
  close();
  return events;
}

function levelsAndEvents(tandems, singles) {
  const levelByLocation = {};
  const eventsByLocation = {};
  const handle = (series, locationId) => {
    const pts = (series.consumption || [])
      .filter((c) => c.binTs != null && c.current != null)
      .map((c) => [new Date(c.binTs), Number(c.current), c.lastValidReadingTs ? new Date(c.lastValidReadingTs) : null, Number(c.capacity) || 0])
      .sort((a, b) => a[0] - b[0]);
    if (!pts.length) return;
    const [binT, cur, validT, cap] = pts[pts.length - 1];
    const seen = (validT || binT).toISOString();
    if (!levelByLocation[locationId]) levelByLocation[locationId] = { lbs: 0, capacity: 0, asOf: null, bins: [] };
    const L = levelByLocation[locationId];
    L.lbs += cur;
    L.capacity += cap;
    L.bins.push({ label: String(series.id), lbs: Math.round(cur), t: seen });
    if (!L.asOf || seen < L.asOf) L.asOf = seen; // oldest bin makes the location read as stale
    for (const ev of detectDeliveries(pts.map((p) => [p[0], p[1]]))) {
      if (!eventsByLocation[locationId]) eventsByLocation[locationId] = [];
      eventsByLocation[locationId].push({ ...ev, bin: String(series.id) });
    }
  };
  for (const series of tandems || []) {
    const loc = TANDEM_ID_TO_LOCATION[series.id];
    if (loc) handle(series, loc);
  }
  for (const series of singles || []) {
    const loc = SENSOR_SERIAL_TO_LOCATION[series.id];
    if (loc) handle(series, loc);
  }
  for (const L of Object.values(levelByLocation)) {
    L.lbs = Math.round(L.lbs);
    L.capacity = Math.round(L.capacity);
  }
  return { levelByLocation, eventsByLocation };
}

// Same merge rule as api/binmaster-sync.js: inside this run's window the fresh
// detection wins; older stored events are kept; events within EVENT_EDGE_MS of
// the window start are ignored from the fresh set (the fill may be clipped).
function mergeEvents(existing, fresh, windowStartMs, nowMs) {
  const cutoff = windowStartMs + EVENT_EDGE_MS;
  const retainFrom = nowMs - EVENT_RETENTION_DAYS * 24 * 60 * 60 * 1000;
  const merged = {};
  const locs = new Set([...Object.keys(existing || {}), ...Object.keys(fresh || {})]);
  for (const loc of locs) {
    const kept = ((existing && existing[loc]) || []).filter((e) => Date.parse(e.t) < cutoff);
    const added = ((fresh && fresh[loc]) || []).filter((e) => Date.parse(e.t) >= cutoff);
    const all = kept.concat(added).filter((e) => Date.parse(e.t) >= retainFrom);
    all.sort((a, b) => Date.parse(a.t) - Date.parse(b.t));
    if (all.length) merged[loc] = all;
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

    // Level + delivery step. Isolated so that any problem here (a field name
    // BarnTools rejects, a timeout) leaves the daily-consumption sync above
    // fully intact; the error is stored under `levelError` for diagnosis.
    let levelByLocation = (stored && stored.levelByLocation) || {};
    let deliveryEvents = (stored && stored.deliveryEvents) || {};
    let eventsFrom = (stored && stored.eventsFrom) || null;
    let levelError = null;
    try {
      const { data: lvData, startDt: lvStart } = await fetchLevelHistory(token, endDt);
      const found = levelsAndEvents(lvData.tandems, lvData.singles);
      if (Object.keys(found.levelByLocation).length) {
        levelByLocation = found.levelByLocation;
        deliveryEvents = mergeEvents(stored && stored.deliveryEvents, found.eventsByLocation, lvStart.getTime(), endDt.getTime());
        const edge = lvStart.getTime() + EVENT_EDGE_MS;
        eventsFrom = new Date(stored && stored.eventsFrom ? Math.min(Date.parse(stored.eventsFrom), edge) : edge).toISOString();
      } else {
        levelError = "level query returned no usable `current` values";
      }
    } catch (e) {
      levelError = String(e && e.message ? e.message : e).slice(0, 500);
    }

    await redis.set(
      "barntools-feed-data",
      JSON.stringify({
        byLocation: mergedByLocation,
        levelByLocation,
        deliveryEvents,
        eventsFrom,
        levelError,
        detection: { minLbs: DELIVERY_MIN_LBS, dipTol: DELIVERY_DIP_TOL },
        unmapped,
        updatedAt: new Date().toISOString(),
      })
    );

    return res.status(200).json({
      ok: true,
      daysFetched: daysBack,
      tandemsRequested: ALL_TANDEM_IDS.length,
      sensorsRequested: ALL_SENSOR_SERIALS.length,
      locations: Object.keys(mergedByLocation).length,
      levelLocations: Object.keys(levelByLocation).length,
      deliveryEventsStored: Object.values(deliveryEvents).reduce((n, a) => n + a.length, 0),
      levelError,
      unmapped,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: String(err && err.message ? err.message : err) });
  }
}
