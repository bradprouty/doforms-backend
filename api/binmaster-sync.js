// api/binmaster-sync.js
//
// Vercel Cron Job target. Runs entirely on Vercel -- no mill computer
// involvement, no Python. On its own schedule (see vercel.json), this:
//   1. Logs into BinMaster's BinCloud API (OAuth2 password grant)
//   2. Pulls the last 10 days of raw bin-level readings for the account
//   3. Derives daily consumption per bin (sum of level *drops* between
//      consecutive readings within a calendar day -- level increases are
//      deliveries, not consumption, and are ignored rather than subtracted)
//   4. Aggregates per FoxPro location via BINCLOUD_VESSEL_MAP below
//   5. Stores the result in Redis under "binmaster-feed-data"
//
// foxpro_sync.py (on the mill computer) reads the result back via a plain
// GET to /api/binmaster-data -- it never talks to BinCloud or holds
// BinCloud credentials itself. This is the JS port of what used to be
// binmaster_adapter.py; that file still exists for local testing/reference
// but is no longer part of the live pipeline.
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
//                        anyone else from hitting this URL and burning your
//                        BinCloud API quota.
//
// Wire the schedule in vercel.json (repo root), e.g. once a day (the
// Hobby-plan minimum interval):
//   { "crons": [{ "path": "/api/binmaster-sync", "schedule": "0 6 * * *" }] }

import { Redis } from "@upstash/redis";

const redis = Redis.fromEnv();

const TOKEN_URL = "https://bincloud.binmaster.com/authorizationserver/connect/token";
const API_BASE = "https://bincloud.binmaster.com/resourceapi/api/public/Vessel";
const DAYS_BACK = 10;

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

export default async function handler(req, res) {
  const { CRON_SECRET } = process.env;
  if (CRON_SECRET) {
    const auth = req.headers["authorization"];
    if (auth !== `Bearer ${CRON_SECRET}`) {
      return res.status(401).json({ error: "unauthorized" });
    }
  }

  try {
    const token = await getAccessToken();
    const endDt = new Date();
    const startDt = new Date(endDt.getTime() - DAYS_BACK * 24 * 60 * 60 * 1000);
    const vessels = await fetchVesselReadingRange(token, process.env.BINCLOUD_ACCOUNT_ID, startDt, endDt);
    const perVessel = consumptionByVesselAndDay(vessels);
    const { byLocation, unmapped } = aggregateByLocation(perVessel);

    await redis.set(
      "binmaster-feed-data",
      JSON.stringify({ byLocation, unmapped, updatedAt: new Date().toISOString() })
    );

    return res.status(200).json({ ok: true, locations: Object.keys(byLocation).length, unmapped });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: String(err && err.message ? err.message : err) });
  }
}
