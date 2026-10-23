// WicketX live-scores sync: CricketData -> Appwrite matches table (server-side only).
// The CricketData API key lives only in this function's environment; it never
// ships in the web app or the APK. Free plan: 100 hits/day, near-live data,
// personal/non-commercial use only (https://cricketdata.org/terms-of-use/).

const ENDPOINT = process.env.APPWRITE_ENDPOINT || 'https://fra.cloud.appwrite.io/v1';
const PROJECT = process.env.APPWRITE_PROJECT_ID;
const DB_ID = process.env.APPWRITE_DATABASE_ID || 'wicketx';
const TABLE = process.env.APPWRITE_MATCHES_TABLE || 'matches';
const CRIC_KEY = process.env.CRICKETDATA_API_KEY;
const APPWRITE_KEY = process.env.APPWRITE_API_KEY;
const DAILY_HIT_BUDGET = 90; // stay under the 100/day free limit with headroom


function istParts(dateTimeGMT) {
  try {
    const d = new Date(dateTimeGMT.endsWith('Z') ? dateTimeGMT : dateTimeGMT + 'Z');
    const time = d.toLocaleTimeString('en-IN', { hour: 'numeric', minute: '2-digit', hour12: true, timeZone: 'Asia/Kolkata' }).toUpperCase() + ' IST';
    const day = d.toLocaleDateString('en-IN', { weekday: 'short', day: 'numeric', month: 'short', timeZone: 'Asia/Kolkata' });
    return { time, day };
  } catch {
    return { time: '', day: '' };
  }
}

function transform(m) {
  const teams = (m.teams || []).slice(0, 2);
  const short = {};
  (m.teamInfo || []).forEach((t) => { short[t.name] = t.shortname || t.name.slice(0, 3).toUpperCase(); });
  const codes = teams.map((t) => short[t] || t.replace(/[^A-Za-z]/g, '').slice(0, 3).toUpperCase());
  // CricAPI inning strings: "Kuwait Inning 1" (names the batting team) or
  // "Qatar,Kuwait Inning 1" (comma form: both teams, used for the side whose
  // innings otherwise appear in no-comma form for the other team).
  const scores = m.score || [];
  const norm = (inn) => String(inn || '').split(',')[0].replace(/\s*Inning\s*\d*.*$/i, '').trim().toLowerCase();
  const hasComma = (inn) => String(inn || '').includes(',');
  const noCommaTeams = new Set(scores.filter((s) => !hasComma(s.inning)).map((s) => norm(s.inning)));
  const commaTeam = (teams.find((t) => !noCommaTeams.has(t.toLowerCase())) || '').toLowerCase();
  const battingTeam = (s) => (hasComma(s.inning) ? commaTeam : norm(s.inning));
  const perTeam = teams.map((team) => {
    const hits = scores.filter((s) => battingTeam(s) === team.toLowerCase());
    if (!hits.length) return { score: 'Yet to bat', overs: '' };
    return {
      score: hits.map((s) => `${s.r}/${s.w}`).join(' & '),
      overs: hits.length === 1 ? String(hits[0].o) : '',
    };
  });
  const status = m.matchEnded ? 'Result' : m.matchStarted ? 'In play' : 'Upcoming';
  const nameParts = String(m.name || '').split(',');
  const series = (nameParts.length > 1 ? nameParts.slice(1).join(',').trim() : 'Cricket') || 'Cricket';
  const fmt = String(m.matchType || '').toLowerCase();
  const { time, day } = istParts(String(m.dateTimeGMT || ''));
  return {
    id: String(m.id),
    format: fmt.includes('t20') ? 'T20' : fmt.includes('test') ? 'Test' : 'ODI',
    status,
    series,
    teams,
    codes,
    scores: perTeam.map((p) => p.score),
    overs: perTeam.map((p) => p.overs),
    note: String(m.status || ''),
    venue: String(m.venue || ''),
    time,
    day,
    ...(/(women)/i.test(String(m.name)) ? { women: true } : {}),
  };
}

async function api(path, opts = {}) {
  const res = await fetch(`${ENDPOINT}${path}`, {
    method: opts.method || 'GET',
    headers: {
      'X-Appwrite-Project': PROJECT,
      'X-Appwrite-Key': APPWRITE_KEY,
      'Content-Type': 'application/json',
    },
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  if (res.status === 404) return null;
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch { json = { raw: text }; }
  if (!res.ok) throw new Error(`Appwrite ${res.status}: ${text.slice(0, 200)}`);
  return json;
}

export default async ({ req, res, log, error }) => {
  if (!CRIC_KEY || !APPWRITE_KEY || !PROJECT) {
    error('Missing env: CRICKETDATA_API_KEY, APPWRITE_API_KEY or APPWRITE_PROJECT_ID');
    return res.json({ ok: false, reason: 'misconfigured' }, 500);
  }
  const cric = await fetch(`https://api.cricapi.com/v1/currentMatches?apikey=${CRIC_KEY}&offset=0`);
  const cricJson = await cric.json();
  const info = cricJson.info || {};
  log(`cricketdata status=${cricJson.status} hitsToday=${info.hitsToday} hitsLimit=${info.hitsLimit}`);
  if (cricJson.status !== 'success' || !Array.isArray(cricJson.data)) {
    error(`CricketData fetch failed: ${JSON.stringify(cricJson).slice(0, 200)}`);
    return res.json({ ok: false, reason: 'provider-failure' }, 502);
  }
  const live = cricJson.data
    .filter((m) => Array.isArray(m.teams) && m.teams.length >= 2 && m.id)
    .map(transform);
  const keep = new Set(live.map((m) => m.id));

  // List existing rows and replace the live set; demo rows are never touched.
  const existing = await api(`/tablesdb/${DB_ID}/tables/${TABLE}/rows?limit=100`);
  const rows = (existing && existing.rows) || [];
  let created = 0, updated = 0, removed = 0, failed = 0;
  for (const m of live) {
    const body = { payload: JSON.stringify(m), dataMode: 'live' };
    try {
      const upd = await api(`/tablesdb/${DB_ID}/tables/${TABLE}/rows/${m.id}`, { method: 'PATCH', body: { data: body } });
      if (upd) { updated++; continue; }
      await api(`/tablesdb/${DB_ID}/tables/${TABLE}/rows`, { method: 'POST', body: { rowId: m.id, data: body } });
      created++;
    } catch (e) { failed++; error(`upsert ${m.id}: ${e.message}`); }
  }
  for (const row of rows) {
    if (row.dataMode === 'live' && !keep.has(row.$id)) {
      try { await api(`/tablesdb/${DB_ID}/tables/${TABLE}/rows/${row.$id}`, { method: 'DELETE' }); removed++; }
      catch (e) { failed++; error(`delete ${row.$id}: ${e.message}`); }
    }
  }
  const overBudget = Number(info.hitsToday || 0) >= DAILY_HIT_BUDGET;
  if (overBudget) log(`daily hit budget reached (${info.hitsToday}); further syncs should skip fetching`);
  return res.json({ ok: failed === 0, live: live.length, created, updated, removed, failed, hitsToday: info.hitsToday, overBudget });
};
