// api/ap-sync.js
//
// Vercel Cron Job target. Runs entirely on Vercel -- no mill computer
// involvement, no Python, no local mailbox software. On its own schedule
// (see vercel.json), this:
//   1. Logs into the dedicated AP-report Gmail mailbox over IMAP
//   2. Grabs the latest email and pulls out its HTML report
//   3. Parses AP's "Daily Consumption" / "Bin Levels" tables per site (same
//      logic as ap_adapter.py, ported here -- see that file's comments for
//      the original Python version and any local testing)
//   4. Aggregates consumption per FoxPro location via AP_BIN_MAP below
//   5. MERGES that into whatever daily history is already stored in Redis
//      under "ap-feed-data", rather than overwriting it -- same reasoning as
//      api/binmaster-sync.js's merge (a phase only gets used once EVERY day
//      of it has real data, so history has to accumulate, not just reflect
//      whatever's in the latest email)
//
// foxpro_sync.py (on the mill computer) reads the result back via a plain
// GET to /api/ap-data -- it never talks to Gmail or holds mailbox
// credentials itself. This replaces the old approach (fetch_ap_report.py +
// ap_adapter.py running on the mill computer against a locally-saved email)
// so that AP, like BinMaster and BarnTools, needs nothing installed or
// scheduled on the mill computer at all. fetch_ap_report.py and
// ap_adapter.py still exist for local testing/reference but are no longer
// part of the live pipeline.
//
// Required Vercel environment variables (Project Settings -> Environment
// Variables -- never commit these):
//   AP_MAILBOX_EMAIL         The dedicated Gmail address created for this.
//   AP_MAILBOX_APP_PASSWORD  That account's 16-character Google "App
//                            Password" (2-Step Verification must be on --
//                            Google blocks plain-password IMAP login).
//   CRON_SECRET              Optional but recommended, shared with the
//                            BinMaster/BarnTools cron routes -- Vercel
//                            automatically sends it as a Bearer token on
//                            cron invocations, and this route checks it so
//                            a stranger who finds the URL can't trigger a
//                            real mailbox login.
//   AP_SENDER_EMAIL          Optional but recommended. AP's own report-
//                            sending address. When set, this route searches
//                            for mail FROM that address instead of just
//                            grabbing the newest message in the inbox --
//                            protects against a stray email (a Google
//                            security/welcome notice, anything else that
//                            lands in this mailbox) being mistaken for the
//                            real report, especially right after the
//                            mailbox is first created. Falls back to
//                            "newest message in the inbox" if unset, which
//                            is only safe for a mailbox that truly never
//                            receives anything else.
//
// Wire the schedule in vercel.json (repo root), offset from the other two
// vendor crons so they don't all fire at once:
//   { "path": "/api/ap-sync", "schedule": "30 6 * * *" }

import { Redis } from "@upstash/redis";
import { ImapFlow } from "imapflow";
import { simpleParser } from "mailparser";

const redis = Redis.fromEnv();

// Bin -> FoxPro location mapping (ported from ap_adapter.py's AP_BIN_MAP --
// keep the two in sync if this ever changes, or just delete the Python copy
// once this route is confirmed live).
const AP_BIN_MAP = {
  "Veldhuizen Pinklet - North Room": {
    A: "15", // Hoogland South
    B: "15", // Hoogland South
    C: "14", // Hoogland North
    D: "14", // Hoogland North
  },
};

function binLabelsFromThead(theadHtml) {
  const cellRe = /<td[^>]*>([\s\S]*?)<\/td>/g;
  const labels = [];
  let m;
  while ((m = cellRe.exec(theadHtml)) !== null) {
    const spans = [...m[1].matchAll(/<span[^>]*>([^<]*)<\/span>/g)].map((s) => s[1].trim());
    const letter = spans[0] || "";
    if (/^[A-Z]$/.test(letter)) labels.push(letter);
  }
  return labels;
}

function parseTable(tableHtml) {
  const theadM = tableHtml.match(/<thead[\s\S]*?<\/thead>/);
  const tbodyM = tableHtml.match(/<tbody[\s\S]*?<\/tbody>/);
  if (!theadM || !tbodyM) return { binLabels: [], data: {} };

  const binLabels = binLabelsFromThead(theadM[0]);
  const data = {};
  const rowRe = /<tr[^>]*>([\s\S]*?)<\/tr>/g;
  let rowMatch;
  while ((rowMatch = rowRe.exec(tbodyM[0])) !== null) {
    const spans = [...rowMatch[1].matchAll(/<span[^>]*>([^<]*)<\/span>/g)].map((s) => s[1].trim());
    if (!spans.length || !/^\d{2}-\d{2}-\d{4}$/.test(spans[0])) continue;
    const [mm, dd, yyyy] = spans[0].split("-");
    const rowDate = `${yyyy}-${mm}-${dd}`; // normalize to YYYY-MM-DD, same convention as BinMaster/BarnTools
    const values = {};
    binLabels.forEach((label, i) => {
      const cell = spans[i + 1];
      if (cell == null) return;
      const numM = cell.match(/([\d.]+)/);
      if (numM) values[label] = parseFloat(numM[1]);
    });
    data[rowDate] = values;
  }
  return { binLabels, data };
}

// Parses a full AP report (one or more site sections) into
// { site_label: { consumption: {date: {bin: lbs}}, level: {...} } }.
function parseApReport(html) {
  const sites = {};
  const sectionHeaderRe = /<td colspan="2"[^>]*>\s*(Daily Consumption|Bin Levels)\s*<\/td>/g;
  const headers = [...html.matchAll(sectionHeaderRe)];

  for (let i = 0; i < headers.length; i++) {
    const headerMatch = headers[i];
    const sectionKind = headerMatch[1];
    const sectionStart = headerMatch.index + headerMatch[0].length;
    const sectionEnd = i + 1 < headers.length ? headers[i + 1].index : html.length;
    const sectionHtml = html.slice(sectionStart, sectionEnd);

    const siteM = sectionHtml.match(/<td colspan="2">\s*([^<]+?)\s*<\/td>/);
    if (!siteM) continue;
    const siteLabel = siteM[1].trim();

    const tableM = sectionHtml.match(/<table[\s\S]*?<\/table>/);
    if (!tableM) continue;
    const { data } = parseTable(tableM[0]);

    const key = sectionKind === "Daily Consumption" ? "consumption" : "level";
    if (!sites[siteLabel]) sites[siteLabel] = { consumption: {}, level: {} };
    Object.assign(sites[siteLabel][key], data);
  }
  return sites;
}

function aggregateByLocation(sites, binMap, kind = "consumption") {
  const byLocation = {};
  const unmapped = new Set();
  for (const [siteLabel, tables] of Object.entries(sites)) {
    const siteMap = binMap[siteLabel];
    if (!siteMap) {
      unmapped.add(siteLabel);
      continue;
    }
    for (const [rowDate, binValues] of Object.entries(tables[kind] || {})) {
      for (const [binLabel, lbs] of Object.entries(binValues)) {
        const loc = siteMap[binLabel];
        if (loc == null) {
          unmapped.add(`${siteLabel} bin ${binLabel}`);
          continue;
        }
        if (!byLocation[loc]) byLocation[loc] = {};
        byLocation[loc][rowDate] = (byLocation[loc][rowDate] || 0) + lbs;
      }
    }
  }
  return { byLocation, unmapped: [...unmapped] };
}

// Same merge-not-overwrite approach as api/binmaster-sync.js -- a day
// present in both is overwritten by the fresh value, every older day only
// present in `existing` is kept.
function mergeByLocation(existing, fresh) {
  const merged = {};
  const allLocations = new Set([...Object.keys(existing || {}), ...Object.keys(fresh || {})]);
  for (const loc of allLocations) {
    merged[loc] = { ...(existing && existing[loc]), ...(fresh && fresh[loc]) };
  }
  return merged;
}

async function fetchLatestReportHtml() {
  const { AP_MAILBOX_EMAIL, AP_MAILBOX_APP_PASSWORD, AP_SENDER_EMAIL } = process.env;
  if (!AP_MAILBOX_EMAIL || !AP_MAILBOX_APP_PASSWORD) {
    throw new Error("AP_MAILBOX_EMAIL and AP_MAILBOX_APP_PASSWORD must both be set");
  }

  const client = new ImapFlow({
    host: "imap.gmail.com",
    port: 993,
    secure: true,
    auth: { user: AP_MAILBOX_EMAIL, pass: AP_MAILBOX_APP_PASSWORD },
    logger: false,
  });

  await client.connect();
  try {
    const lock = await client.getMailboxLock("INBOX");
    try {
      let source = null;

      if (AP_SENDER_EMAIL) {
        // Search for mail from AP's own sending address rather than
        // trusting "the newest message in the inbox" -- see AP_SENDER_EMAIL
        // in the header comment above for why.
        const uids = await client.search({ from: AP_SENDER_EMAIL }, { uid: true });
        if (!uids || !uids.length) {
          throw new Error(`No mail found from ${AP_SENDER_EMAIL} yet`);
        }
        const latestUid = Math.max(...uids);
        for await (const msg of client.fetch(latestUid, { source: true }, { uid: true })) {
          source = msg.source;
        }
      } else {
        // No AP_SENDER_EMAIL configured -- fall back to "grab the newest
        // message in the inbox," which only gives correct results if this
        // mailbox truly never receives anything except AP's reports.
        const total = client.mailbox.exists;
        if (!total) {
          throw new Error("No emails found in this mailbox yet");
        }
        for await (const msg of client.fetch(`${total}:${total}`, { source: true })) {
          source = msg.source;
        }
      }

      if (!source) {
        throw new Error("Could not fetch the latest message");
      }
      const parsed = await simpleParser(source);
      const html = parsed.html || parsed.textAsHtml;
      if (!html) {
        throw new Error("Latest email has no HTML content");
      }
      return { html, subject: parsed.subject, date: parsed.date };
    } finally {
      lock.release();
    }
  } finally {
    await client.logout();
  }
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
    const { html, subject, date } = await fetchLatestReportHtml();
    const sites = parseApReport(html);
    const { byLocation: freshByLocation, unmapped } = aggregateByLocation(sites, AP_BIN_MAP, "consumption");

    const storedRaw = await redis.get("ap-feed-data");
    const stored = storedRaw ? (typeof storedRaw === "string" ? JSON.parse(storedRaw) : storedRaw) : null;
    const mergedByLocation = mergeByLocation(stored && stored.byLocation, freshByLocation);

    await redis.set(
      "ap-feed-data",
      JSON.stringify({ byLocation: mergedByLocation, unmapped, updatedAt: new Date().toISOString() })
    );

    return res.status(200).json({
      ok: true,
      emailSubject: subject,
      emailDate: date,
      sitesSeen: Object.keys(sites).length,
      locations: Object.keys(mergedByLocation).length,
      unmapped,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: String(err && err.message ? err.message : err) });
  }
}
