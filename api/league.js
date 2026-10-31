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
  const [league, standings, hist, games, roast] = await Promise.all([
    redis.get("league"), redis.get("standings"), redis.hgetall("history"), redis.get("games"), redis.get("roast"),
  ]);
  const history = Object.values(hist || {}).map(parse)
    .sort((a, b) => (a.date < b.date ? -1 : 1)).slice(-200); // the whole season, for the Analytics chart
  return { league: parse(league) || null, standings: parse(standings) || null, history, games: parse(games) || null, roast: parse(roast) || null };
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

// ---------- Daily Roast ----------
// One roast per day, picked on the server so everyone sees the same one. Every roast is built from an
// opener + closer pair, and each pair is used at most once per season (tracked in the "roast-used" set),
// so the same insult never shows up twice. When possible the opener calls out the actual loss that sank
// the target's day: the opponent, the score, upsets against a worse team, and the losing team's top scorer.
const LINE = {ATL:43.5,BOS:51.5,BKN:24.5,CHA:39.5,CHI:29.5,CLE:48.5,DAL:34.5,DEN:49.5,DET:49.5,GSW:39.5,HOU:48.5,IND:44.5,LAC:27.5,LAL:46.5,MEM:28.5,MIA:46.5,MIL:26.5,MIN:48.5,NOP:27.5,NYK:52.5,OKC:60.5,ORL:43.5,PHI:50.5,PHX:40.5,POR:43.5,SAC:20.5,SAS:59.5,TOR:45.5,UTA:35.5,WAS:35.5};
const NICK = {ATL:"Hawks",BOS:"Celtics",BKN:"Nets",CHA:"Hornets",CHI:"Bulls",CLE:"Cavaliers",DET:"Pistons",IND:"Pacers",MIA:"Heat",MIL:"Bucks",NYK:"Knicks",ORL:"Magic",PHI:"76ers",TOR:"Raptors",WAS:"Wizards",DAL:"Mavericks",DEN:"Nuggets",GSW:"Warriors",HOU:"Rockets",LAC:"Clippers",LAL:"Lakers",MEM:"Grizzlies",MIN:"Timberwolves",NOP:"Pelicans",OKC:"Thunder",PHX:"Suns",POR:"Trail Blazers",SAC:"Kings",SAS:"Spurs",UTA:"Jazz"};
const ord = n => n + (["th","st","nd","rd"][((n % 100) - 20) % 10] || ["th","st","nd","rd"][n % 100] || "th");
const plural = (n, w) => `${n} ${w}${n === 1 ? "" : "s"}`;

function pacificHour() {
  return +new Intl.DateTimeFormat("en-US", { timeZone: "America/Los_Angeles", hour: "numeric", hour12: false }).format(new Date()) % 24;
}
function dayBefore(date) {
  const d = new Date(date + "T12:00:00Z"); d.setUTCDate(d.getUTCDate() - 1); return d.toISOString().slice(0, 10);
}

// Final scores for one date, with each team's top scorer.
async function fetchResults(date) {
  const r = await fetch(`https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=${date.replace(/-/g, "")}`, { signal: AbortSignal.timeout(5000) });
  if (!r.ok) throw new Error(`ESPN returned ${r.status}`);
  const j = await r.json();
  const MAP = { GS: "GSW", NO: "NOP", NY: "NYK", SA: "SAS", UTAH: "UTA", WSH: "WAS" };
  const ab = c => MAP[c?.team?.abbreviation] || c?.team?.abbreviation;
  const topScorer = c => {
    const cat = (c.leaders || []).find(l => l.name === "points" || l.abbreviation === "PTS");
    const L = cat?.leaders?.[0];
    const name = L?.athlete?.displayName || L?.athlete?.shortName;
    const pts = Math.round(+(L?.value ?? parseFloat(L?.displayValue)));
    return name && Number.isFinite(pts) ? { name, pts } : null;
  };
  const out = [];
  for (const ev of j.events || []) {
    const comp = ev.competitions?.[0] || {};
    const st = comp.status?.type || ev.status?.type || {};
    if (st.state !== "post") continue;
    const cs = comp.competitors || [];
    const win = cs.find(c => c.winner), lose = cs.find(c => c !== win);
    if (!win || !lose || !TEAM_KEYS.includes(ab(win)) || !TEAM_KEYS.includes(ab(lose))) continue;
    const ws = +win.score, ls = +lose.score;
    out.push({ winner: ab(win), loser: ab(lose), ws, ls, margin: ws - ls, loserTop: topScorer(lose) });
  }
  return out;
}

const pick = a => a[Math.floor(Math.random() * a.length)];

// Openers always name the player. "need" says when an opener makes sense.
const GAME_OPENERS = [
  { id: "g1", need: g => g.upset, t: c => `${c.P}'s ${c.T} lost to the ${c.O}, a team Vegas expected to win ${c.OL} games` },
  { id: "g2", need: g => g.margin >= 10, t: c => `${c.P}'s ${c.T} got run off the floor by the ${c.O}, ${c.S}` },
  { id: "g3", need: g => g.loserTop, t: c => `${c.star} scored ${c.pts} as ${c.P}'s ${c.T} lost to the ${c.O}` },
  { id: "g4", need: g => g.loserTop && g.loserTop.pts < 20, t: c => `${c.star} led ${c.P}'s ${c.T} with a whopping ${c.pts} points in a loss to the ${c.O}` },
  { id: "g5", need: g => g.upset, t: c => `${c.P}'s ${c.T} somehow lost to the ${c.O} ${c.S}` },
  { id: "g6", need: g => g.margin >= 15, t: c => `${c.P}'s ${c.T} lost by ${c.M} to the ${c.O}` },
  { id: "g7", need: () => true, t: c => `The ${c.O} beat ${c.P}'s ${c.T} ${c.S}` },
  { id: "g8", need: g => g.margin <= 5, t: c => `${c.P}'s ${c.T} found a way to lose a ${c.M}-point game to the ${c.O}` },
  { id: "g9", need: g => LINE[g.winner] < 32, t: c => `${c.P}'s ${c.T} lost to the tanking ${c.O}` },
  { id: "g10", need: g => g.loserTop && g.loserTop.pts >= 25, t: c => `${c.P}'s ${c.T} lost ${c.S} to the ${c.O} even with ${c.star} dropping ${c.pts}` },
  { id: "g11", need: () => true, t: c => `${c.P} needed the ${c.T} last night and got a ${c.S} loss to the ${c.O}` },
];
const PLAIN_OPENERS = [
  { id: "n1", need: c => c.day, t: c => `${c.P} added ${plural(c.dw, "win")} yesterday while everyone else moved on with their lives` },
  { id: "n2", need: c => c.rank > 1, t: c => `${c.P} is ${ord(c.rank)} out of ${c.n}` },
  { id: "n3", need: c => c.gb >= 1, t: c => `${c.P} sits ${plural(c.gb, "win")} out of first` },
  { id: "n4", need: c => c.played, t: c => `${c.P}'s roster is a combined ${c.rec}` },
  { id: "n5", need: c => c.played, t: c => `${c.P}'s ${c.WT} are ${c.WR}` },
  { id: "n6", need: c => c.day, t: c => `${c.P} went ${c.dw}-${c.dl} yesterday` },
  { id: "n7", need: () => true, t: c => `${c.P} drafted the ${c.WT} with a straight face` },
  { id: "n8", need: () => true, t: c => `${c.P} built this roster on purpose` },
  { id: "n9", need: c => c.rank === c.n, t: c => `${c.P} is in last place` },
];
const CLOSERS = [
  "and the group chat has officially stopped pretending it's bad luck",
  "which is about what you'd expect from someone who drafts with their eyes closed",
  "and even the teams that are tanking on purpose feel bad for them",
  "and Vegas is thinking about taking them off the board out of pity",
  "so maybe it's time to let a coin flip make their picks",
  "and their roster is now the cautionary tale we tell new drafters",
  "and the only thing falling faster than their team is their credibility",
  "and the standings have stopped being polite about it",
  "which is the least surprising thing to happen since draft night",
  "and they still had the nerve to show up in the group chat",
  "and somewhere a lottery front office is taking notes",
  "and the trophy has filed a restraining order",
  "proof that confidence and basketball knowledge are two different things",
  "and it's starting to look like they drafted off the 2006 standings",
  "and their picks are aging worse than gas station sushi",
  "and the season is long, but not that long",
  "which explains why nobody asks them for picks anymore",
  "and the draft board would like a word",
  "so at least they're consistent",
  "and the only direction they've moved all season is backwards",
  "and the rest of the league sends its thoughts and prayers",
  "and that's not a slump, that's a lifestyle",
  "and they've officially become the league's favorite free win",
  "and their basketball takes should come with a warning label",
  "which is impressive in the worst possible way",
  "and the scouting report on their drafting is just a shrug emoji",
  "and every other player in this league is quietly thanking them",
];

async function buildRoast() {
  const [league, standings, hist] = await Promise.all([redis.get("league"), redis.get("standings"), redis.hgetall("history")]);
  const L = parse(league), S = parse(standings);
  if (!L || L.status !== "complete" || !S?.teams) return null;
  const history = Object.values(hist || {}).map(parse).sort((a, b) => (a.date < b.date ? -1 : 1));
  const prev = [...history].reverse().find(h => h.date < S.asOf);
  const rec = (src, a) => ({ w: +(src?.teams?.[a]?.w || 0), l: +(src?.teams?.[a]?.l || 0) });
  const rows = L.players.map((name, i) => {
    const teams = L.picks.filter(p => p.p === i).map(p => p.t);
    let w = 0, l = 0, pw = 0, pl = 0;
    teams.forEach(a => { const r = rec(S, a), q = rec(prev, a); w += r.w; l += r.l; pw += q.w; pl += q.l; });
    return { name, teams, w, l, dw: prev ? w - pw : null, dl: prev ? l - pl : null };
  }).sort((x, y) => y.w - x.w || x.l - y.l || x.name.localeCompare(y.name));
  const lead = rows[0];
  rows.forEach((r, k) => { r.gb = lead.w - r.w; r.rank = k > 0 && rows[k - 1].w === r.w && rows[k - 1].l === r.l ? rows[k - 1].rank : k + 1; });

  // Target: worst day if days differ, otherwise last place.
  let target = rows[rows.length - 1], why = "Last place", day = false;
  if (prev && rows.some(r => r.dw !== rows[0].dw || r.dl !== rows[0].dl)) {
    target = [...rows].sort((a, b) => a.dw - b.dw || b.dl - a.dl || a.w - b.w)[0];
    why = "Worst day in the league"; day = true;
  }

  // The loss that hurt most: an upset against a worse team, then the bigger margin.
  let game = null;
  try {
    const results = await fetchResults(dayBefore(todayPacific()));
    const losses = results.filter(g => target.teams.includes(g.loser)).map(g => ({ ...g, upset: LINE[g.loser] - LINE[g.winner] >= 4 }));
    losses.sort((a, b) => (LINE[b.loser] - LINE[b.winner]) + b.margin / 2 - ((LINE[a.loser] - LINE[a.winner]) + a.margin / 2));
    game = losses[0] || null;
  } catch (e) { /* no game details today; plain roast */ }

  const played = rows.some(r => r.w + r.l > 0);
  const worstTeam = [...target.teams].sort((a, b) => {
    const x = rec(S, a), y = rec(S, b), px = x.w + x.l ? x.w / (x.w + x.l) : .5, py = y.w + y.l ? y.w / (y.w + y.l) : .5;
    return px - py || LINE[a] - LINE[b];
  })[0];
  const wr = rec(S, worstTeam);
  const c = {
    P: target.name, rank: target.rank, n: rows.length, gb: target.gb, rec: `${target.w}-${target.l}`, played, day,
    dw: target.dw, dl: target.dl, WT: NICK[worstTeam], WR: `${wr.w}-${wr.l}`,
  };
  if (game) Object.assign(c, {
    T: NICK[game.loser], O: NICK[game.winner], S: `${game.ws}-${game.ls}`, M: game.margin, OL: LINE[game.winner],
    star: game.loserTop?.name, pts: game.loserTop?.pts,
  });

  const openers = [
    ...(game ? GAME_OPENERS.filter(o => o.need(game)) : []),
    ...PLAIN_OPENERS.filter(o => o.need(c)),
  ];
  const used = new Set((await redis.smembers("roast-used")) || []);
  let pairs = [];
  for (const o of openers) CLOSERS.forEach((cl, k) => { const key = `${o.id}|${k}`; if (!used.has(key)) pairs.push({ o, k, key }); });
  if (!pairs.length) return null;
  // Lean on the actual game when there is one: 3 out of 4 days, if a fresh game pair exists.
  const gamePairs = pairs.filter(p => p.o.id[0] === "g");
  if (gamePairs.length && Math.random() < 0.75) pairs = gamePairs;
  const choice = pick(pairs);
  const text = `${choice.o.t(c)}, ${CLOSERS[choice.k]}.`;
  await redis.sadd("roast-used", choice.key);
  return { player: target.name, text: text[0].toUpperCase() + text.slice(1), why, game: game ? `${game.winner} ${game.ws}, ${game.loser} ${game.ls}` : null };
}

// Builds today's roast once, after last night's games are final (6 AM Pacific).
async function ensureRoast() {
  try {
    const date = todayPacific();
    const cur = parse(await redis.get("roast"));
    if (cur && cur.date === date) return;
    if (pacificHour() < 6) return;
    const claimed = await redis.set("roast-lock", "1", { nx: true, px: 60000 });
    if (!claimed) return;
    const r = await buildRoast();
    if (r) await redis.set("roast", { date, ...r, createdAt: new Date().toISOString() });
  } catch (e) {
    // keep yesterday's roast if anything goes wrong
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
    await ensureRoast();
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
    await ensureRoast();
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
