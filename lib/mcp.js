// lib/mcp.js
// Read-only MCP (Model Context Protocol) server so Claude can read the
// Active Groups and Treatments data already stored in Redis.
//
// Lives in lib/ (not api/) so it doesn't use one of the 12 Hobby functions;
// api/treatments.js hands requests to it when action=mcp. vercel.json
// rewrites the friendly URL:
//
//   https://doforms-backend.vercel.app/mcp/<MCP_KEY>
//
// Add that URL in claude.ai -> Settings -> Connectors -> Add custom connector.
//
// Env vars:
//   MCP_KEY   any long random string; it is part of the connector URL above.
//
// Never calls doForms itself: it only reads what webhook.js / treatments.js
// already stored, so it can't trigger doForms rate limits.

const PROTOCOL_VERSIONS = ['2025-06-18', '2025-03-26', '2024-11-05'];
const SERVER_INFO = { name: 'veldhuizen-doforms', title: 'Veldhuizen doForms', version: '1.0.0' };

// Field names confirmed from real doForms records (same as treatments.html).
const TX_FIELDS = {
  group: {
    date: 'G_Treat_Date', barn: 'Barn_Name', group: 'G_Treat_Group_ID', treatment: 'Treat_Name',
    type: 'Type', category: 'FDA_Category', head: 'Group_Number_Head',
    qty: 'Treat_G_Qty', unit: 'Application_Unit', cost: 'Total_Treatment_Cost', lines: null,
  },
  individual: {
    date: 'I_Treat_Date', barn: 'Barn_Name', group: 'I_Treat_Group_ID', treatment: 'Treat_Name',
    type: 'Type', category: 'FDA_Category', head: 'Ind_Number_Head',
    qty: 'Ind_Total_Doses', unit: 'Application_Unit', cost: 'Total_Treatment_Cost', lines: 'Ind_Treatments',
  },
};
const TX_KEYS = { group: 'doforms-group-treatments', individual: 'doforms-individual-treatments' };

const TOOLS = [
  {
    name: 'list_active_groups',
    title: 'List active pig groups',
    description:
      'Lists active pig groups from the doForms Active Groups dashboard: group ID, description, location (barn/site), ' +
      'feed phase, current head, mortality %, start date and days on feed. Optional search filters by group ID, ' +
      'description or location.',
    inputSchema: {
      type: 'object',
      properties: {
        search: { type: 'string', description: 'Text to match in group ID, description or location.' },
      },
    },
    annotations: { readOnlyHint: true, openWorldHint: false },
  },
  {
    name: 'get_group_health',
    title: 'Get a group with its treatments',
    description:
      'Returns one active group (all doForms fields plus feed progress) and its group and individual treatment ' +
      'records from the doForms treatment forms, newest first. Use the group ID from list_active_groups.',
    inputSchema: {
      type: 'object',
      properties: {
        group_id: { type: 'string', description: 'Group ID, e.g. 2634NN2EN.' },
        days: { type: 'number', description: 'Only treatments from the last N days. Default 180; 0 = all.' },
      },
      required: ['group_id'],
    },
    annotations: { readOnlyHint: true, openWorldHint: false },
  },
];

// ---------- data helpers ----------

const norm = (v) => String(v ?? '').trim().toUpperCase();
const num = (v) => {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

function daysSince(dateStr) {
  const t = Date.parse(dateStr);
  if (!Number.isFinite(t)) return null;
  return Math.floor((Date.now() - t) / 86400000);
}

// Same doForms <-> FoxPro match as api/dashboard-data.js.
async function loadGroups(redis) {
  const [doformsData, feedData] = await Promise.all([redis.get('doforms-data'), redis.get('foxpro-feed-data')]);
  const groups = Array.isArray(doformsData) ? doformsData : [];
  const feeds = feedData && Array.isArray(feedData.groups) ? feedData.groups : [];
  const byAuxLoc = new Map();
  const byAux = new Map();
  for (const f of feeds) {
    const aux = norm(f.aux_id);
    byAuxLoc.set(`${aux}::${String(f.location ?? '').trim()}`, f);
    if (!byAux.has(aux)) byAux.set(aux, []);
    byAux.get(aux).push(f);
  }
  return {
    generatedAt: feedData ? feedData.generated_at : null,
    groups: groups.map((g) => {
      const id = norm(g.GroupID);
      let feed = byAuxLoc.get(`${id}::${String(g.Location ?? '').trim()}`);
      if (!feed && (byAux.get(id) || []).length === 1) feed = byAux.get(id)[0];
      return { raw: g, feed: feed || null };
    }),
  };
}

function summarizeGroup({ raw, feed }) {
  const mort = num(raw.Mortality_percent);
  return {
    group_id: raw.GroupID ?? null,
    description: raw.Group_Description ?? null,
    location: raw.Location ?? null,
    phase: feed?.phase ?? null,
    current_head: num(raw.Current_Inventory),
    mortality_pct: mort === null ? null : Math.round(mort * 1000) / 10,
    start_date: raw.Start_Date ?? null,
    days_on_feed: daysSince(raw.Start_Date),
    predicted_weight_lb: num(feed?.predicted_weight),
    expected_close_date: feed?.expected_close_date ?? null,
  };
}

// Plain scalar doForms fields only (no tables, files or internal ids).
function scalarFields(raw) {
  const out = {};
  for (const [k, v] of Object.entries(raw || {})) {
    if (k.startsWith('_')) continue;
    if (v === null || typeof v === 'object' || v === '[file]') continue;
    out[k] = v;
  }
  return out;
}

function treatmentRows(form, rec) {
  const f = TX_FIELDS[form];
  const base = {
    form,
    date: rec[f.date] ?? rec._createTime ?? null,
    barn: rec[f.barn] ?? null,
    group_id: rec[f.group] ?? null,
    treatment: rec[f.treatment] ?? null,
    type: rec[f.type] ?? null,
    fda_category: rec[f.category] ?? null,
    head_treated: num(rec[f.head]),
    qty: num(rec[f.qty]),
    unit: rec[f.unit] ?? null,
    cost: num(rec[f.cost]),
    entered_by: rec._enteredBy ?? null,
  };
  const lines = f.lines && Array.isArray(rec[f.lines]) ? rec[f.lines] : null;
  if (!lines || !lines.length) return [base];
  // Individual form: one row per treatment line, line fields win over the header.
  return lines.map((line) => ({
    ...base,
    treatment: line[f.treatment] ?? base.treatment,
    type: line[f.type] ?? base.type,
    fda_category: line[f.category] ?? base.fda_category,
    head_treated: num(line[f.head]) ?? base.head_treated,
    qty: num(line[f.qty]) ?? num(line.Dose) ?? base.qty,
    unit: line[f.unit] ?? base.unit,
    cost: num(line[f.cost]) ?? base.cost,
  }));
}

async function loadTreatments(redis, groupId, days) {
  const want = norm(groupId);
  const cutoff = days > 0 ? Date.now() - days * 86400000 : null;
  const rows = [];
  for (const form of Object.keys(TX_KEYS)) {
    const recs = (await redis.hgetall(TX_KEYS[form])) || {};
    for (const rec of Object.values(recs)) {
      if (norm(rec[TX_FIELDS[form].group]) !== want) continue;
      for (const row of treatmentRows(form, rec)) {
        const t = Date.parse(row.date);
        if (cutoff && Number.isFinite(t) && t < cutoff) continue;
        rows.push(row);
      }
    }
  }
  rows.sort((a, b) => (Date.parse(b.date) || 0) - (Date.parse(a.date) || 0));
  return rows;
}

// ---------- tools ----------

async function callTool(redis, name, args = {}) {
  if (name === 'list_active_groups') {
    const { generatedAt, groups } = await loadGroups(redis);
    const q = String(args.search ?? '').trim().toLowerCase();
    const list = groups
      .map(summarizeGroup)
      .filter((g) => !q || [g.group_id, g.description, g.location].some((v) => String(v ?? '').toLowerCase().includes(q)))
      .sort((a, b) => String(a.group_id).localeCompare(String(b.group_id)));
    return { feed_data_as_of: generatedAt, count: list.length, groups: list };
  }

  if (name === 'get_group_health') {
    const groupId = String(args.group_id ?? '').trim();
    if (!groupId) throw toolError('group_id is required');
    const days = args.days === undefined ? 180 : Math.max(0, Number(args.days) || 0);
    const { generatedAt, groups } = await loadGroups(redis);
    const matches = groups.filter((g) => norm(g.raw.GroupID) === norm(groupId));
    const treatments = await loadTreatments(redis, groupId, days);
    if (!matches.length && !treatments.length) {
      throw toolError(`No active group or treatments found for "${groupId}". Use list_active_groups to see group IDs.`);
    }
    return {
      feed_data_as_of: generatedAt,
      group_id: groupId,
      locations: matches.map((m) => ({
        ...summarizeGroup(m),
        doforms_fields: scalarFields(m.raw),
        feed: m.feed,
      })),
      treatments_window_days: days || 'all',
      treatment_count: treatments.length,
      treatments: treatments.slice(0, 200),
    };
  }

  throw toolError(`Unknown tool: ${name}`);
}

function toolError(message) {
  const e = new Error(message);
  e.isToolError = true;
  return e;
}

// ---------- JSON-RPC ----------

async function handleMessage(redis, msg) {
  const { id, method, params } = msg || {};
  const isNotification = id === undefined || id === null;
  const reply = (result) => ({ jsonrpc: '2.0', id, result });
  const fail = (code, message) => ({ jsonrpc: '2.0', id: id ?? null, error: { code, message } });

  if (!msg || msg.jsonrpc !== '2.0' || typeof method !== 'string') return fail(-32600, 'Invalid request');

  switch (method) {
    case 'initialize': {
      const asked = params?.protocolVersion;
      return reply({
        protocolVersion: PROTOCOL_VERSIONS.includes(asked) ? asked : PROTOCOL_VERSIONS[0],
        capabilities: { tools: { listChanged: false } },
        serverInfo: SERVER_INFO,
        instructions:
          'Read-only Veldhuizen Farms pig-group data from doForms: active groups and their group/individual treatments.',
      });
    }
    case 'ping':
      return reply({});
    case 'tools/list':
      return reply({ tools: TOOLS });
    case 'tools/call': {
      try {
        const data = await callTool(redis, params?.name, params?.arguments || {});
        return reply({ content: [{ type: 'text', text: JSON.stringify(data) }], structuredContent: data, isError: false });
      } catch (e) {
        if (!e.isToolError) console.error('mcp tool failed', e);
        return reply({ content: [{ type: 'text', text: e.isToolError ? e.message : 'Server error reading stored data.' }], isError: true });
      }
    }
    default:
      if (isNotification) return null; // notifications/initialized etc.
      return fail(-32601, `Method not found: ${method}`);
  }
}

export async function handleMcp(req, res, redis) {
  // No 401 here on purpose: a 401 makes Claude assume the server uses OAuth
  // and try to register a sign-in, which fails with a confusing message.
  const key = process.env.MCP_KEY;
  const given = (req.query || {}).mcpkey;
  if (!key) {
    return res.status(503).json({ error: 'MCP_KEY is not set on this deployment. Add it in Vercel, then redeploy.' });
  }
  if (given !== key) return res.status(404).json({ error: 'Not found' });

  if (req.method === 'GET') return res.status(405).setHeader('Allow', 'POST').end(); // no server-sent stream
  if (req.method === 'DELETE') return res.status(200).end();                        // stateless: nothing to end
  if (req.method !== 'POST') return res.status(405).end();

  let body = req.body;
  if (typeof body === 'string') {
    try { body = JSON.parse(body); } catch { return res.status(400).json({ jsonrpc: '2.0', id: null, error: { code: -32700, message: 'Parse error' } }); }
  }

  if (Array.isArray(body)) {
    const out = (await Promise.all(body.map((m) => handleMessage(redis, m)))).filter(Boolean);
    return out.length ? res.status(200).json(out) : res.status(202).end();
  }
  const out = await handleMessage(redis, body);
  return out ? res.status(200).json(out) : res.status(202).end();
}
