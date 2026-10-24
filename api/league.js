// NBA Wins Draft: all server code in one file.
// /api/state, /api/action and /api/cron-standings are routed here by vercel.json.
import { Redis } from "@upstash/redis";
import { timingSafeEqual } from "node:crypto";

// Finds the database keys whatever prefix the Vercel/Upstash integration gave them
// (UPSTASH_REDIS_REST_URL, KV_REST_API_URL, STORAGE_KV_REST_API_URL, ...).
function findEnv(re, notRe) {
  const k = Object.keys(process.env).find(k => re.test(k) && !(notRe && notRe.test(k)) && process.env[k]);
  return k ? process.env[k] : undefined;
}
const DB_URL = findEnv(/(REDIS_REST_URL|REST_API_URL)$/);
const DB_TOKEN = findEnv(/(REDIS_REST_TOKEN|REST_API_TOKEN)$/, /READ_ONLY/);
const redis = new Redis({ url: DB_URL || "https://missing.invalid", token: DB_TOKEN || "missing" });

const TEAM_KEYS = ["ATL","BOS","BKN","CHA","CHI","CLE","DET","IND","MIA","MIL","NYK","ORL","PHI","TOR","WAS",
  "DAL","DEN","GSW","HOU","LAC","LAL","MEM","MIN","NOP","OKC","PHX","POR","SAC","SAS","UTA"];

const parse = v => (typeof v === "string" ? JSON.parse(v) : v);

async function getState() {
  const [league, standings, hist, games] = await Promise.all([
    redis.get("league"), redis.get("standings"), redis.hgetall("history"), redis.get("games"),
  ]);
  const history = Object.values(hist || {}).map(parse)
    .sort((a, b) => (a.date < b.date ? -1 : 1)).slice(-200); // the whole season, for the Analytics chart
  return { league: parse(league) || null, standings: parse(standings) || null, history, games: parse(games) || null };
}

async function saveStandings(teams, date, source) {
  await redis.set("standings", { asOf: date, updatedAt: new Date().toISOString(), source, teams });
  await redis.hset("history", { [date]: JSON.stringify({ date, teams }) });
}

function todayPacific() {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "America/Los_Angeles" }).format(new Date());
}

// Pulls regular-season W-L for all 30 teams from ESPN's public standings feed.
async function fetchStandings() {
  const season = process.env.SEASON || "2027"; // ESPN names a season by its ending year: 2027 = 2026-27
  const r = await fetch(`https://site.api.espn.com/apis/v2/sports/basketball/nba/standings?season=${season}&seasontype=2`, { signal: AbortSignal.timeout(5000) });
  if (!r.ok) throw new Error(`ESPN returned ${r.status}`);
  const j = await r.json();
  const MAP = { GS: "GSW", NO: "NOP", NY: "NYK", SA: "SAS", UTAH: "UTA", WSH: "WAS" };
  const teams = {};
  for (const conf of j.children || []) {
    for (const e of conf.standings?.entries || []) {
      const ab = MAP[e.team?.abbreviation] || e.team?.abbreviation;
      const stat = n => Math.round(+(e.stats?.find(s => s.name === n)?.value || 0));
      if (TEAM_KEYS.includes(ab)) teams[ab] = { w: stat("wins"), l: stat("losses") };
    }
  }
  const missing = TEAM_KEYS.filter(k => !teams[k]);
  if (missing.length) throw new Error("Standings feed is missing: " + missing.join(", "));
  return teams;
}

// Today's games (Pacific date) from ESPN's public scoreboard feed, for the Matchup of the Day tab.
async function fetchGames(date) {
  const r = await fetch(`https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=${date.replace(/-/g, "")}`, { signal: AbortSignal.timeout(5000) });
  if (!r.ok) throw new Error(`ESPN returned ${r.status}`);
  const j = await r.json();
  const MAP = { GS: "GSW", NO: "NOP", NY: "NYK", SA: "SAS", UTAH: "UTA", WSH: "WAS" };
  const ab = c => MAP[c?.team?.abbreviation] || c?.team?.abbreviation;
  const games = [];
  for (const ev of j.events || []) {
    const comp = ev.competitions?.[0] || {};
    const cs = comp.competitors || [];
    const home = cs.find(c => c.homeAway === "home"), away = cs.find(c => c.homeAway === "away");
    if (!home || !away || !TEAM_KEYS.includes(ab(home)) || !TEAM_KEYS.includes(ab(away))) continue;
    const st = comp.status?.type || ev.status?.type || {};
    const win = cs.find(c => c.winner);
    games.push({ away: ab(away), home: ab(home), start: ev.date || null, state: st.state || "pre",
      detail: st.shortDetail || "", winner: st.state === "post" && win ? ab(win) : null });
  }
  return games;
}

// Keeps the saved schedule fresh: reloads it on a new day, or every 10 minutes so live/final status stays current.
async function refreshGamesIfStale() {
  try {
    const g = parse(await redis.get("games"));
    const date = todayPacific();
    if (g && g.date === date && Date.now() - Date.parse(g.updatedAt) < REFRESH_MS) return;
    const claimed = await redis.set("games-lock", "1", { nx: true, px: 30000 });
    if (!claimed) return;
    await redis.set("games", { date, updatedAt: new Date().toISOString(), games: await fetchGames(date) });
  } catch (e) {
    // ESPN slow or down: keep the last saved schedule
  }
}

const same = (a, b) => {
  const x = Buffer.from(String(a)), y = Buffer.from(String(b));
  return x.length === y.length && timingSafeEqual(x, y);
};
const fail = (res, status, error, extra = {}) => res.status(status).json({ error, ...extra });


async function stateRoute(req, res) {
  res.setHeader("Cache-Control", "no-store");
  if (!DB_URL || !DB_TOKEN) {
    return res.status(500).json({ error: "No database connected. In Vercel: Storage → connect Upstash Redis to this project, then Redeploy." });
  }
  try {
    await Promise.all([refreshIfStale(), refreshGamesIfStale()]);
    res.status(200).json(await getState());
  } catch (e) {
    res.status(500).json({ error: "Database found but not reachable: " + String(e.message || e) });
  }
}

// Live updates: whenever someone has the site open, re-pull standings from ESPN
// if the saved copy is more than 10 minutes old, so wins show up soon after each game ends.
const REFRESH_MS = 10 * 60 * 1000;
async function refreshIfStale() {
  try {
    const st = parse(await redis.get("standings"));
    if (st && st.updatedAt && Date.now() - Date.parse(st.updatedAt) < REFRESH_MS) return;
    // Wins typed in by hand stick for 6 hours before live updates take over again
    if (st && st.source === "manual" && Date.now() - Date.parse(st.updatedAt) < 6 * 3600 * 1000) return;
    const claimed = await redis.set("refresh-lock", "1", { nx: true, px: 30000 });
    if (!claimed) return; // another visitor is already refreshing
    const teams = await fetchStandings();
    await saveStandings(teams, todayPacific(), "auto");
  } catch (e) {
    // ESPN slow or down: keep showing the last saved standings
  }
}

async function cronRoute(req, res) {
  if (process.env.CRON_SECRET && req.headers.authorization !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  try {
    const teams = await fetchStandings();
    const date = todayPacific();
    await saveStandings(teams, date, "auto");
    await refreshGamesIfStale();
    res.status(200).json({ ok: true, date, teams });
  } catch (e) {
    res.status(502).json({ error: String(e.message || e) });
  }
}

async function actionRoute(req, res) {
  if (req.method !== "POST") return fail(res, 405, "Use POST.");
  const LP = process.env.LEAGUE_PASSWORD, AP = process.env.ADMIN_PASSWORD;
  if (!LP || !AP) return fail(res, 500, "The site owner needs to set LEAGUE_PASSWORD and ADMIN_PASSWORD in Vercel.");

  const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : (req.body || {});
  const { action, password = "" } = body;
  const role = same(password, AP) ? "admin" : same(password, LP) ? "player" : null;
  if (!role) return fail(res, 401, "Wrong password.");
  if (action === "check") return res.json({ ok: true, role });
  if ((action === "reset" || action === "restart" || action === "wins") && role !== "admin") {
    return fail(res, 403, "Only the commissioner can do that.");
  }

  // One change at a time, so two people can't grab the same pick.
  const locked = await redis.set("lock", "1", { nx: true, px: 5000 });
  if (!locked) return fail(res, 409, "Another change is saving. Try again in a second.");
  try {
    let league = await redis.get("league");
    if (typeof league === "string") league = JSON.parse(league);

    if (action === "setup") {
      if (league) return fail(res, 409, "A league already exists.", { state: await getState() });
      const players = (body.players || []).map(p => String(p).trim().slice(0, 24)).filter(Boolean);
      if (players.length < 2 || players.length > 10) return fail(res, 400, "Use 2 to 10 players.");
      if (new Set(players.map(p => p.toLowerCase())).size !== players.length) return fail(res, 400, "Each player needs a different name.");
      const rounds = players.length > 6 ? 3 : 4; // everyone drafts the same number of teams
      await redis.set("league", { players, rounds, picks: [], status: "drafting", season: "2026-27", createdAt: new Date().toISOString() });

    } else if (action === "pick") {
      if (!league || league.status !== "drafting") return fail(res, 409, "The draft isn't open.", { state: await getState() });
      if (body.expected !== league.picks.length) return fail(res, 409, "The board changed. Here's the latest.", { state: await getState() });
      const team = String(body.team || "");
      if (!TEAM_KEYS.includes(team)) return fail(res, 400, "Unknown team.");
      if (league.picks.some(p => p.t === team)) return fail(res, 409, "That team was just taken.", { state: await getState() });
      const n = league.players.length, idx = league.picks.length, round = Math.floor(idx / n), pos = idx % n;
      // Snake with third-round reversal: R1 1→N, R2 N→1, R3 N→1, R4 1→N
      const forward = round === 0 || (round >= 3 && round % 2 === 1);
      const player = forward ? pos : n - 1 - pos;
      league.picks.push({ p: player, t: team, at: new Date().toISOString() });
      league.status = league.picks.length >= n * league.rounds ? "complete" : "drafting";
      await redis.set("league", league);

    } else if (action === "undo") {
      if (!league || !league.picks.length) return fail(res, 409, "There's no pick to undo.", { state: await getState() });
      if (body.expected !== league.picks.length) return fail(res, 409, "The board changed. Here's the latest.", { state: await getState() });
      league.picks.pop();
      league.status = "drafting";
      await redis.set("league", league);

    } else if (action === "restart") {
      if (!league) return fail(res, 409, "There's no league to restart.", { state: await getState() });
      league.picks = [];
      league.status = "drafting";
      await redis.set("league", league);

    } else if (action === "reset") {
      await redis.del("league");

    } else if (action === "wins") {
      const date = /^\d{4}-\d{2}-\d{2}$/.test(body.date || "") ? body.date : new Date().toISOString().slice(0, 10);
      const teams = {};
      for (const k of TEAM_KEYS) {
        const w = +body.teams?.[k]?.w, l = +body.teams?.[k]?.l;
        if (!Number.isInteger(w) || !Number.isInteger(l) || w < 0 || l < 0 || w + l > 82) return fail(res, 400, `Check the record for ${k}.`);
        teams[k] = { w, l };
      }
      await saveStandings(teams, date, "manual");

    } else {
      return fail(res, 400, "Unknown action.");
    }
    return res.json({ ok: true, role, state: await getState() });
  } catch (e) {
    return fail(res, 500, "Couldn't save. Try again.");
  } finally {
    await redis.del("lock");
  }
}

export default async function handler(req, res) {
  const op = req.query?.op;
  if (op === "state") return stateRoute(req, res);
  if (op === "cron") return cronRoute(req, res);
  if (op === "action") return actionRoute(req, res);
  return res.status(404).json({ error: "Not found" });
}
