// api/inventory-data.js
//
// Serves the doForms group inventory (Current_Inventory, Sold, ...) together with a
// timestamped history of every change, so the Feed Logistics dashboard can work out
// how many pigs were in each barn on each day.
//
// Why a history: doForms only stores today's Current_Inventory (and a running Sold
// total), not when pigs were sold. Feed eaten per day depends on how many pigs were
// there that day, so the dashboard needs the head count per day, not just today's.
//
// How it works:
//   GET  /api/inventory-data                 read-only. Returns { records, history }.
//   GET  /api/inventory-data?snapshot=1      same, but FIRST compares the inventory
//                                            doForms currently holds against the last
//                                            stored state and, if anything changed,
//                                            appends one event { t, changes }.
//                                            Needs  Authorization: Bearer <CRON_SECRET>
//                                            (the 15-minute GitHub Actions workflow
//                                            calls it, so a sale is time-stamped to
//                                            within ~15 minutes of the doForms resync).
//
// The first snapshot records every group as a baseline event. Before that moment the
// dashboard assumes the head count was constant.
//
// Reads Redis key "doforms-data" (written by api/webhook.js) and writes
// "inventory-history". Uses the same Upstash Redis as the other routes. Needs no new
// environment variables. Counts as one more serverless function on the Vercel Hobby
// plan (limit 12).

import { Redis } from "@upstash/redis";

const redis = Redis.fromEnv();

const HISTORY_KEY = "inventory-history";
const MAX_EVENTS = 3000;

const asJson = (v) => (v == null ? null : typeof v === "string" ? JSON.parse(v) : v);

function toNum(x) {
  if (x == null || x === "") return null;
  const n = parseFloat(String(x).replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

// One group per GroupID + Location (Raak has two records that share an id).
const keyOf = (r) => `${String(r.GroupID || "").trim()}|${String(r.Location || "").trim()}`;

function slim(r) {
  return {
    key: keyOf(r),
    GroupID: r.GroupID ?? null,
    Group_Description: r.Group_Description ?? null,
    Location: r.Location ?? null,
    Center: r.Center ?? null,
    Start_Date: r.Start_Date ?? null,
    Starting_Inventory: toNum(r.Starting_Inventory),
    Current_Inventory: toNum(r.Current_Inventory),
    Sold: toNum(r.Sold),
    Died: toNum(r.Died),
    Euthanized: toNum(r.Euthanized),
    Moved_In: toNum(r.Moved_In),
    Moved_Out: toNum(r.Moved_Out),
  };
}

// Fold the event list into the latest known head count per key.
function lastState(events) {
  const state = {};
  for (const ev of events) for (const [k, n] of Object.entries(ev.changes || {})) state[k] = n;
  return state;
}

export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Cache-Control", "no-store");

  try {
    const records = (asJson(await redis.get("doforms-data")) || []).filter((r) => r && r.GroupID);
    const current = records.map(slim);
    let history = asJson(await redis.get(HISTORY_KEY)) || { events: [] };
    if (!Array.isArray(history.events)) history = { events: [] };

    let snapshot = null;
    if (req.query && req.query.snapshot) {
      const { CRON_SECRET } = process.env;
      if (CRON_SECRET && req.headers["authorization"] !== `Bearer ${CRON_SECRET}`) {
        return res.status(401).json({ error: "unauthorized" });
      }
      // an empty or tiny doForms read must never be recorded as "everyone sold out"
      const prev = lastState(history.events);
      const known = Object.keys(prev).length;
      if (current.length === 0 || (known > 0 && current.length < known / 2)) {
        snapshot = { recorded: false, reason: `doForms returned ${current.length} records (previously ${known}); skipped` };
      } else {
        const changes = {};
        for (const g of current) {
          if (g.Current_Inventory == null) continue;
          if (prev[g.key] !== g.Current_Inventory) changes[g.key] = g.Current_Inventory;
        }
        if (Object.keys(changes).length) {
          history.events.push({ t: new Date().toISOString(), baseline: known === 0 || undefined, changes });
          if (history.events.length > MAX_EVENTS) history.events = history.events.slice(-MAX_EVENTS);
          await redis.set(HISTORY_KEY, JSON.stringify(history));
          snapshot = { recorded: true, changed: Object.keys(changes).length, baseline: known === 0 };
        } else {
          snapshot = { recorded: false, reason: "no change" };
        }
      }
    }

    return res.status(200).json({
      updatedAt: new Date().toISOString(),
      records: current,
      history: history.events,
      snapshot,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: String(err && err.message ? err.message : err) });
  }
}
