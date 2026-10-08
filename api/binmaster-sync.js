// api/binmaster-sync.js
//
// Vercel Cron Job target. Runs entirely on Vercel -- no mill computer
// involvement, no Python. On its own schedule (see vercel.json), this:
//   1. Logs into BinMaster's BinCloud API (OAuth2 password grant)
//   2. Pulls the last `days` days of raw bin-level readings for the account
//      (default 10 -- override with ?days=N for a one-time deeper backfill,
//      see note below)
//   3. Derives daily consumption per bin (sum of level *drops* between
//      consecutive readings within a calendar day -- level increases are
//      deliveries, not consumption, and are ignored rather than subtracted)
//   4. Aggregates per FoxPro location via BINCLOUD_VESSEL_MAP below
//   5. MERGES that into whatever daily history is already stored in Redis
//      under "binmaster-feed-data", rather than overwriting it -- see
//      "Why this accumulates" below
//
// foxpro_sync.py (on the mill computer) reads the result back via a plain
// GET to /api/binmaster-data -- it never talks to BinCloud or holds
// BinCloud credentials itself. This is the JS port of what used to be
// binmaster_adapter.py; that file still exists for local testing/reference
// but is no longer part of the live pipeline.
//
// Why this accumulates instead of overwriting (added 2026-08-31):
// foxpro_sync.py only trusts BinMaster's number for a phase if EVERY day of
// that phase has real data -- a phase that's only 90% covered still falls
// back to the FoxPro delivery estimate rather than risk understating it.
// Finisher phases run for weeks; if this route only ever kept the last 10
// days, no in-progress phase could ever be fully covered and BinMaster data
// would silently never actually get used. So each run merges its freshly
// fetched days into the full history already in Redis (new days overwrite
// same-day values in case of late corrections; every older day is kept).
// Over time the daily 10-day fetch is just resilience against a missed cron
// run -- the real coverage comes from history that never gets thrown away.
//
// One-time backfill for phases already in progress when this was deployed:
// call this route once with a larger window, e.g.
//   GET /api/binmaster-sync?days=120
// (requires the CRON_SECRET Bearer header, same as any other call to this
// route -- see below). How far back BinCloud actually has data is unknown
// until you try; the response's `daysFetched`/`vesselsSeen` fields and the
// Vercel function log will show what came back.
//
// Added 2026-10-08 (feed logistics dashboard): besides daily consumption this
// route now ALSO stores, in the same Redis record:
//   levelByLocation  { loc: { lbs, asOf, bins: [{label, lbs, t}] } }
//                    the most recent reading of every bin, summed per location.
//                    Replaced (not merged) every run.
//   deliveryEvents   { loc: [{ t, lbs, bin }] }
//                    feed deliveries detected as sustained level RISES in a
//                    single bin (see detectDeliveries). Merged run to run and
//                    kept for EVENT_RETENTION_DAYS.
// Existing fields (byLocation, unmapped, updatedAt) are unchanged, so
// foxpro_sync.py and api/dashboard-data are unaffected.
//
// Required Vercel environment variables (Project Settings -> Environment
// Variables -- never commit these):
//   BINCLOUD_CLIENT_ID
//   BINCLOUD_USERNAME
//   BINCLOUD_PASSWORD
//   BINCLOUD_ACCOUNT_ID
//   CRON_SECRET          Optional but recommended. When set, Vercel
//                        automatically sends it as a Bearer token on cron
//                        invocations, and this route checks it -- stops
//                        anyone else from hitting this URL (including for a
//                        large ?days= backfill) and burning your BinCloud
//                        API quota.
//
// Wire the schedule in vercel.json (repo root), e.g. once a day (the
// Hobby-plan minimum interval):
//   { "crons": [{ "path": "/api/binmaster-sync", "schedule": "0 6 * * *" }] }

import { Redis } from "@upstash/redis";

const redis = Redis.fromEnv();

const TOKEN_URL = "https://bincloud.binmaster.com/authorizationserver/connect/token";
const API_BASE = "https://bincloud.binmaster.com/resourceapi/api/public/Vessel";
const DEFAULT_DAYS_BACK = 10;
const MAX_DAYS_BACK = 400; // sanity cap on manual ?days= backfills

// vesselId -> [FoxPro LOCATION_I, human label] -- confirmed against
// m_locat.dbf / farm_bin.dbf capacities on 2026-08-28 (see binmaster_adapter.py
// for the full validation notes). Two vessels (bins) per location; their
// readings are summed into that location's daily total.
//
// NOTE: Schaap North (11) and Schaap South (12) are intentionally not here --
// their bin-scale data comes from a different vendor, not BinCloud.
const BINCLOUD_VESSEL_MAP = {
  // BROUWER FINISHER
  "efe45b1a-6733-403b-adc3-82485635fac4": ["21", "Brouwer North"],
  "4c60d084-c163-423b-9a4c-f54d520fc128": ["21", "Brouwer South"],
  // DAKOTA NORTH FINISHER
  "b7f9676c-7711-4a66-9848-a8d7f6bae4b5": ["19", "Dakota North North"],
  "91377880-37ac-4d8a-bfbc-3aac0071474b": ["19", "Dakota North South"],
  // DAKOTA SOUTH FINISHER
  "e9a7eaba-441b-4c7c-a4f5-6821f7dcc16c": ["18", "Dakota South North"],
  "4ac944bd-1951-4280-bd44-208404ee8b7b": ["18", "Dakota South South"],
  // DAKOTA WEST FINISHER
  "ef670149-b20c-42bd-91bb-ba364299f5b6": ["17", "Dakota West North"],
  "925e9a35-6723-4044-b08c-47ad0f4d7763": ["17", "Dakota West South"],
  // ERF SOUTH FINISHER
  "a1d0d379-0c84-4535-b1af-f4f0720dff73": ["2", "ERF South North"],
  "51679ce1-caec-484b-881d-e56ca70e3f7c": ["2", "ERF South South"],
  // ERF NORTH FINISHER (bins physically replaced/upsized to 25,000 lb;
  // FoxPro capacity updated 2026-08-28 to match)
  "fd8542e4-7ac6-4d0e-a374-84136831d7ae": ["3", "ERF North North"],
  "9e9b5530-3ffb-46bc-9db5-106639ca1ef8": ["3", "ERF North South"],
  // YARD FINISHER
  "48ea5f0d-cc1c-4570-9ce0-259aff92961a": ["5", "Yard North"],
  "ce007915-2778-474a-9ecf-50657fc08555": ["5", "Yard South"],
  // PAULSEN SOUTH
  "cad38ffd-f0bd-4861-8229-215137df931a": ["23", "Paulsen South North"],
  "29427a73-a8cb-499d-a700-c663507f1176": ["23", "Paulsen South South"],
  // PAULSEN NORTH
  "c2d5d26d-6a17-4b43-969c-b92d46359076": ["24", "Paulsen North North"],
  "b579e146-d85a-4b7f-9877-335a22e49057": ["24", "Paulsen North South"],
  // SCHAAP WEST ("DW West" is BinMaster's own internal building label --
  // confirmed by exact 25,000 lb capacity match, not by name)
  "0f3bb07a-413b-4602-9bdc-59f524553ca4": ["913", "DW West Inside"],
  "850358ea-b16e-420b-8d31-c55a64bddd33": ["913", "DW West Outside"],
  // SCHAAP EAST ("DW East")
  "27e7f4b8-9473-4b99-b645-c91bb34d0967": ["914", "DW East Inside"],
  "1a8bfdf0-dc39-401f-a8f6-9ffab524fb98": ["914", "DW East Outside"],
};

function fmtBinCloudTimestamp(d) {
  // BinCloud wants YYYY-MM-DDTHH:MM:SS (no milliseconds/zone suffix).
  return d.toISOString().slice(0, 19);
}

function dateKey(d) {
  return d.toISOString().slice(0, 10); // YYYY-MM-DD (UTC)
}

async function getAccessToken() {
  const { BINCLOUD_CLIENT_ID, BINCLOUD_USERNAME, BINCLOUD_PASSWORD } = process.env;
  if (!BINCLOUD_CLIENT_ID || !BINCLOUD_USERNAME || !BINCLOUD_PASSWORD) {
    throw new Error("BINCLOUD_CLIENT_ID, BINCLOUD_USERNAME and BINCLOUD_PASSWORD must all be set");
  }
  const body = new URLSearchParams({
    grant_type: "password",
    resource: "resourceApi",
    client_id: BINCLOUD_CLIENT_ID,
    username: BINCLOUD_USERNAME,
    password: BINCLOUD_PASSWORD,
    client_secret: "",
  });
  const resp = await fetch(TOKEN_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
  });
  if (!resp.ok) {
    throw new Error(`BinCloud token request failed: ${resp.status} ${await resp.text()}`);
  }
  const data = await resp.json();
  return data.access_token;
}

async function fetchVesselReadingRange(token, accountId, startDt, endDt) {
  const url =
    `${API_BASE}/GetVesselsReadingRangeByAccountId/${accountId}/` +
    `${fmtBinCloudTimestamp(startDt)}/${fmtBinCloudTimestamp(endDt)}`;
  const resp = await fetch(url, {
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
  });
  if (!resp.ok) {
    throw new Error(`BinCloud reading-range request failed: ${resp.status} ${await resp.text()}`);
  }
  return resp.json();
}

function readingsForVessel(vessel) {
  const out = [];
  for (const sensor of vessel.sensors || []) {
    for (const r of sensor.readings || []) {
      if (r.timestamp == null || r.mass == null) continue;
      out.push([new Date(r.timestamp), Number(r.mass)]);
    }
  }
  out.sort((a, b) => a[0] - b[0]);
  return out;
}

function consumptionByVesselAndDay(vessels) {
  const result = {};
  for (const vessel of vessels) {
    const vesselId = vessel.vesselId;
    const readings = readingsForVessel(vessel);
    const byDay = {};
    for (let i = 0; i < readings.length - 1; i++) {
      const m1 = readings[i][1];
      const [t2, m2] = readings[i + 1];
      const drop = m1 - m2;
      if (drop > 0) {
        const key = dateKey(t2);
        byDay[key] = (byDay[key] || 0) + drop;
      }
    }
    result[vesselId] = byDay;
  }
  return result;
}

function aggregateByLocation(vesselConsumption) {
  const byLocation = {};
  const unmapped = new Set();
  for (const [vesselId, dayTotals] of Object.entries(vesselConsumption)) {
    const mapped = BINCLOUD_VESSEL_MAP[vesselId];
    if (!mapped) {
      unmapped.add(vesselId);
      continue;
    }
    const [locationId] = mapped;
    if (!byLocation[locationId]) byLocation[locationId] = {};
    for (const [day, lbs] of Object.entries(dayTotals)) {
      byLocation[locationId][day] = (byLocation[locationId][day] || 0) + lbs;
    }
  }
  return { byLocation, unmapped: [...unmapped] };
}

// Merges `fresh` (this run's byLocation) into `existing` (everything ever
// stored) day-by-day. A day present in both is overwritten by the fresh
// value (in case BinCloud corrected a late-arriving reading); every day
// only present in `existing` is kept untouched. This is what lets a
// long-running phase eventually become fully covered even though each run
// only fetches a short recent window.
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
// A delivery shows up as a bin's level climbing steadily over a stretch of
// readings (an auger/blower fill takes minutes to hours). Drops are normal
// consumption, so a "fill" is a run of readings that go up, tolerating small
// dips (sensor noise, feed leaving the bin mid-fill). The run ends when the
// level falls DELIVERY_DIP_TOL below its peak, or there is a data gap. The
// delivery size is peak minus the level where the run started. Runs smaller
// than DELIVERY_MIN_LBS are treated as noise and discarded.
// Tune these three constants if the dashboard shows phantom or missed fills.
const DELIVERY_MIN_LBS = 1500;
const DELIVERY_DIP_TOL = 400;
const DELIVERY_MAX_GAP_MS = 2 * 60 * 60 * 1000;
const EVENT_RETENTION_DAYS = 75;
const EVENT_EDGE_MS = 6 * 60 * 60 * 1000; // events this close to the window start may be clipped

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

function levelsAndEvents(vessels) {
  const levelByLocation = {};
  const eventsByLocation = {};
  for (const vessel of vessels) {
    const mapped = BINCLOUD_VESSEL_MAP[vessel.vesselId];
    if (!mapped) continue;
    const [loc, label] = mapped;
    const readings = readingsForVessel(vessel);
    if (!readings.length) continue;

    const [lastT, lastMass] = readings[readings.length - 1];
    if (!levelByLocation[loc]) levelByLocation[loc] = { lbs: 0, asOf: null, bins: [] };
    const L = levelByLocation[loc];
    L.lbs += lastMass;
    L.bins.push({ label, lbs: Math.round(lastMass), t: lastT.toISOString() });
    // asOf = the OLDEST bin timestamp at this location, so a single stale
    // sensor makes the whole location read as stale rather than hiding it.
    if (!L.asOf || lastT.toISOString() < L.asOf) L.asOf = lastT.toISOString();

    for (const ev of detectDeliveries(readings)) {
      if (!eventsByLocation[loc]) eventsByLocation[loc] = [];
      eventsByLocation[loc].push({ ...ev, bin: label });
    }
  }
  for (const L of Object.values(levelByLocation)) L.lbs = Math.round(L.lbs);
  return { levelByLocation, eventsByLocation };
}

// Merge freshly detected events into stored history. Inside this run's fetch
// window the fresh detection is authoritative (it sees whole fills); events
// older than the window are kept; events within EVENT_EDGE_MS of the window
// start are ignored from the fresh set because the fill may be clipped.
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
    const vessels = await fetchVesselReadingRange(token, process.env.BINCLOUD_ACCOUNT_ID, startDt, endDt);
    const perVessel = consumptionByVesselAndDay(vessels);
    const { byLocation: freshByLocation, unmapped } = aggregateByLocation(perVessel);

    const storedRaw = await redis.get("binmaster-feed-data");
    const stored = storedRaw ? (typeof storedRaw === "string" ? JSON.parse(storedRaw) : storedRaw) : null;
    const mergedByLocation = mergeByLocation(stored && stored.byLocation, freshByLocation);

    const { levelByLocation, eventsByLocation } = levelsAndEvents(vessels);
    const deliveryEvents = mergeEvents(
      stored && stored.deliveryEvents,
      eventsByLocation,
      startDt.getTime(),
      endDt.getTime()
    );

    // Earliest moment the stored delivery events can be trusted from. Lets the
    // dashboard tell "no delivery happened" apart from "we weren't watching yet".
    const edge = startDt.getTime() + EVENT_EDGE_MS;
    const eventsFrom = new Date(
      stored && stored.eventsFrom ? Math.min(Date.parse(stored.eventsFrom), edge) : edge
    ).toISOString();

    await redis.set(
      "binmaster-feed-data",
      JSON.stringify({
        byLocation: mergedByLocation,
        levelByLocation,
        deliveryEvents,
        eventsFrom,
        detection: { minLbs: DELIVERY_MIN_LBS, dipTol: DELIVERY_DIP_TOL },
        unmapped,
        updatedAt: new Date().toISOString(),
      })
    );

    return res.status(200).json({
      ok: true,
      daysFetched: daysBack,
      vesselsSeen: vessels.length,
      locations: Object.keys(mergedByLocation).length,
      levelLocations: Object.keys(levelByLocation).length,
      deliveryEventsStored: Object.values(deliveryEvents).reduce((n, a) => n + a.length, 0),
      unmapped,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: String(err && err.message ? err.message : err) });
  }
}
