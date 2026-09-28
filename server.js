require("dotenv").config();
const express = require("express");
const http = require("http");
const path = require("path");
const crypto = require("crypto");
const { Server } = require("socket.io");
const { Pool } = require("pg");

const app = express();
const server = http.createServer(app);
const io = new Server(server);
const PORT = Number(process.env.PORT || 3000);
const missing = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"].filter(k => !process.env[k]);
if (missing.length) { console.error("Missing .env values:", missing.join(", ")); process.exit(1); }

const pool = new Pool({
  host: process.env.DB_HOST, port: Number(process.env.DB_PORT), database: process.env.DB_NAME,
  user: process.env.DB_USER, password: process.env.DB_PASSWORD,
  ssl: { rejectUnauthorized: false }, max: 10, idleTimeoutMillis: 30000, connectionTimeoutMillis: 10000
});
const ADMIN_USERNAME = process.env.ADMIN_USERNAME || "admin";
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || "change-this-password";
const ACTIVE_MS = 10000;

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

function dbError(res, e) { console.error(e); return res.status(500).json({ error: "Database operation failed.", detail: process.env.NODE_ENV === "production" ? undefined : e.message }); }
function adminAuth(req, res, next) {
  const h = req.headers.authorization || "";
  if (!h.startsWith("Basic ")) { res.setHeader("WWW-Authenticate", 'Basic realm="Prayer Admin"'); return res.status(401).json({ error: "Admin authentication required." }); }
  const decoded = Buffer.from(h.slice(6), "base64").toString("utf8"), i = decoded.indexOf(":");
  const u = i >= 0 ? decoded.slice(0, i) : "", p = i >= 0 ? decoded.slice(i + 1) : "";
  if (u !== ADMIN_USERNAME || p !== ADMIN_PASSWORD) { res.setHeader("WWW-Authenticate", 'Basic realm="Prayer Admin"'); return res.status(401).json({ error: "Invalid admin credentials." }); }
  next();
}
async function activePeople() { const r = await pool.query(`SELECT id,name,session_token,joined_at,last_seen FROM participants WHERE last_seen >= $1 ORDER BY joined_at`, [new Date(Date.now() - ACTIVE_MS)]); return r.rows; }
async function emitRoom() { const p = await activePeople(); io.emit("room-count", p.length); io.emit("room-participants-changed", p.map(({ id, name, joined_at }) => ({ id, name, joined_at }))); return p.length; }
function emitData() { io.emit("prayer-data-changed"); }
async function broadcast() { emitData(); try { await emitRoom(); } catch (e) { console.error("room broadcast", e.message); } }
async function reactionCounts(ids) { if (!ids.length) return {}; const r = await pool.query(`SELECT prayer_id,COUNT(*)::int AS count FROM prayer_reactions WHERE prayer_id=ANY($1::bigint[]) GROUP BY prayer_id`, [ids]); return Object.fromEntries(r.rows.map(x => [String(x.prayer_id), x.count])); }
async function publicPrayers() { const r = await pool.query(`SELECT id,name,request,visibility,created_at,answered_at FROM prayers WHERE visibility='public' ORDER BY CASE WHEN answered_at IS NULL THEN 0 ELSE 1 END,created_at DESC`); const c = await reactionCounts(r.rows.map(x => x.id)); return r.rows.map(x => ({ ...x, prayer_count: c[String(x.id)] || 0 })); }
async function assignWaiting() { const people = await activePeople(); if (!people.length) return; const a = (await pool.query(`SELECT prayer_id,participant_id,completed_at FROM assignments`)).rows; const assigned = new Set(a.map(x => String(x.prayer_id))); const waiting = (await pool.query(`SELECT id FROM prayers WHERE visibility='public' AND answered_at IS NULL ORDER BY created_at ASC`)).rows.filter(x => !assigned.has(String(x.id))); if (!waiting.length) return; const counts = new Map(people.map(p => [String(p.id), a.filter(x => String(x.participant_id) === String(p.id) && !x.completed_at).length])); for (const prayer of waiting) { people.sort((x, y) => (counts.get(String(x.id)) || 0) - (counts.get(String(y.id)) || 0)); const person = people[0]; try { await pool.query(`INSERT INTO assignments(prayer_id,participant_id) VALUES($1,$2)`, [prayer.id, person.id]); counts.set(String(person.id), (counts.get(String(person.id)) || 0) + 1); } catch (e) { if (e.code !== "23505") throw e; } } }
async function maintenance() { try { await assignWaiting(); await emitRoom(); } catch (e) { console.error("maintenance", e.message); } }
setInterval(maintenance, 2000);

app.post("/api/prayers", async (req, res) => { try { const name = String(req.body.name || "").trim() || null, request = String(req.body.request || "").trim(), visibility = req.body.visibility === "private" ? "private" : "public"; if (!request) return res.status(400).json({ error: "Prayer request is required." }); const r = await pool.query(`INSERT INTO prayers(name,request,visibility) VALUES($1,$2,$3) RETURNING id`, [name, request, visibility]); if (visibility === "public") await assignWaiting(); await broadcast(); res.status(201).json({ id: r.rows[0].id, message: visibility === "public" ? "Your public prayer request was submitted." : "Your private prayer request was submitted. Only administrators can see it." }); } catch (e) { dbError(res, e); } });
app.get("/api/public/prayers", async (req, res) => { try { res.json(await publicPrayers()); } catch (e) { dbError(res, e); } });
app.get("/api/public/stats", async (req, res) => { try { const [a, b, c, room] = await Promise.all([pool.query(`SELECT COUNT(*)::int count FROM prayers WHERE visibility='public' AND answered_at IS NULL`), pool.query(`SELECT COUNT(*)::int count FROM prayers WHERE visibility='public' AND answered_at IS NOT NULL`), pool.query(`SELECT COUNT(*)::int count FROM prayer_reactions`), activePeople().then(x => x.length)]); res.json({ activePrayers: a.rows[0].count, answeredPrayers: b.rows[0].count, prayedCount: c.rows[0].count, roomCount: room }); } catch (e) { dbError(res, e); } });
app.post("/api/prayers/:id/react", async (req, res) => { try { const id = Number(req.params.id), token = String(req.body.reactorToken || "").trim(); if (!Number.isInteger(id) || id <= 0 || !token) return res.status(400).json({ error: "Valid prayer ID and reactor token are required." }); const p = await pool.query(`SELECT id FROM prayers WHERE id=$1 AND visibility='public'`, [id]); if (!p.rowCount) return res.status(404).json({ error: "Prayer not found." }); let participant = null; if (token.startsWith("room_")) { participant = (await pool.query(`SELECT id FROM participants WHERE session_token=$1`, [token])).rows[0]?.id || null; } let reacted = true; try { await pool.query(`INSERT INTO prayer_reactions(prayer_id,participant_id,reactor_token) VALUES($1,$2,$3)`, [id, participant, token]); } catch (e) { if (e.code === "23505") reacted = false; else throw e; } const c = await pool.query(`SELECT COUNT(*)::int count FROM prayer_reactions WHERE prayer_id=$1`, [id]); if (reacted) await broadcast(); res.json({ reacted, count: c.rows[0].count }); } catch (e) { dbError(res, e); } });

// Publicly marking a public prayer answered moves it to Answered Prayers. It is NOT deleted.
app.post("/api/prayers/:id/answered", async (req, res) => { try { const id = Number(req.params.id); if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: "Invalid prayer ID." }); const r = await pool.query(`UPDATE prayers SET answered_at=COALESCE(answered_at,NOW()) WHERE id=$1 AND visibility='public' RETURNING id,answered_at`, [id]); if (!r.rowCount) return res.status(404).json({ error: "Public prayer not found." }); await broadcast(); res.json({ success: true, answered_at: r.rows[0].answered_at }); } catch (e) { dbError(res, e); } });

app.get("/api/admin/prayers", adminAuth, async (req, res) => {
  try {

    const visibility = [
      "all",
      "public",
      "private"
    ].includes(
      String(req.query.visibility || "all")
    )
      ? String(req.query.visibility || "all")
      : "all";


    const answered = [
      "all",
      "active",
      "answered"
    ].includes(
      String(req.query.answered || "all")
    )
      ? String(req.query.answered || "all")
      : "all";


    const sort = [
      "newest",
      "oldest",
      "most_prayed",
      "least_prayed"
    ].includes(
      String(req.query.sort || "newest")
    )
      ? String(req.query.sort || "newest")
      : "newest";


    const date =
      String(req.query.date || "").trim();


    const search =
      String(req.query.search || "").trim();


    const where = [];
    const params = [];


    /* ==========================================
       VISIBILITY FILTER
       ========================================== */

    if (visibility !== "all") {

      params.push(visibility);

      where.push(
        `p.visibility = $${params.length}`
      );

    }


    /* ==========================================
       ACTIVE / ANSWERED FILTER
       ========================================== */

    if (answered === "active") {

      where.push(
        `p.answered_at IS NULL`
      );

    }


    if (answered === "answered") {

      where.push(
        `p.answered_at IS NOT NULL`
      );

    }


    /* ==========================================
       DATE FILTER
       ========================================== */

    if (date) {

      params.push(date);

      const dateParam =
        params.length;


      where.push(
        `p.created_at >= $${dateParam}::date
         AND p.created_at < ($${dateParam}::date + INTERVAL '1 day')`
      );

    }


    /* ==========================================
       SEARCH FILTER
       ========================================== */

    if (search) {

      params.push(`%${search}%`);

      const searchParam =
        params.length;


      where.push(
        `(p.request ILIKE $${searchParam}
          OR COALESCE(p.name, '') ILIKE $${searchParam})`
      );

    }


    /* ==========================================
       SORT
       ========================================== */

    let orderBy =
      "p.created_at DESC";


    if (sort === "oldest") {

      orderBy =
        "p.created_at ASC";

    }


    if (sort === "most_prayed") {

      orderBy =
        "prayer_count DESC, p.created_at DESC";

    }


    if (sort === "least_prayed") {

      orderBy =
        "prayer_count ASC, p.created_at DESC";

    }


    /* ==========================================
       QUERY
       ========================================== */

    const sql = `

      SELECT
        p.id,
        p.name,
        p.request,
        p.visibility,
        p.created_at,
        p.answered_at,

        COUNT(pr.id)::int AS prayer_count

      FROM prayers p

      LEFT JOIN prayer_reactions pr
        ON pr.prayer_id = p.id

      ${where.length
        ? "WHERE " + where.join(" AND ")
        : ""
      }

      GROUP BY p.id

      ORDER BY ${orderBy}

    `;


    const result =
      await pool.query(
        sql,
        params
      );


    res.json(result.rows);


  } catch (e) {

    dbError(res, e);

  }
});
app.get("/api/admin/stats", adminAuth, async (req, res) => { try { const r = await pool.query(`SELECT COUNT(*)::int all_count,COUNT(*) FILTER(WHERE visibility='public')::int public_count,COUNT(*) FILTER(WHERE visibility='private')::int private_count,COUNT(*) FILTER(WHERE answered_at IS NOT NULL)::int answered_count,COUNT(*) FILTER(WHERE visibility='public' AND answered_at IS NULL)::int active_count FROM prayers`), react = await pool.query(`SELECT COUNT(*)::int count FROM prayer_reactions`), room = await activePeople(); res.json({ all: r.rows[0].all_count, public: r.rows[0].public_count, private: r.rows[0].private_count, answered: r.rows[0].answered_count, active: r.rows[0].active_count, reactions: react.rows[0].count, room: room.length }); } catch (e) { dbError(res, e); } });
app.get("/api/admin/participants", adminAuth, async (req, res) => { try { res.json(await activePeople()); } catch (e) { dbError(res, e); } });
app.post("/api/admin/prayers/:id/toggle-answered", adminAuth, async (req, res) => { try { const id = Number(req.params.id); const cur = await pool.query(`SELECT id,answered_at FROM prayers WHERE id=$1`, [id]); if (!cur.rowCount) return res.status(404).json({ error: "Prayer not found." }); const next = cur.rows[0].answered_at ? null : new Date(); await pool.query(`UPDATE prayers SET answered_at=$1 WHERE id=$2`, [next, id]); await broadcast(); res.json({ success: true, answered: Boolean(next) }); } catch (e) { dbError(res, e); } });

// ONLY ADMIN CAN PERMANENTLY DELETE.
app.delete("/api/admin/prayers/:id", adminAuth, async (req, res) => { const client = await pool.connect(); try { const id = Number(req.params.id); if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: "Invalid prayer ID." }); await client.query("BEGIN"); const p = await client.query(`SELECT id FROM prayers WHERE id=$1 FOR UPDATE`, [id]); if (!p.rowCount) { await client.query("ROLLBACK"); return res.status(404).json({ error: "Prayer not found." }); } await client.query(`DELETE FROM prayer_reactions WHERE prayer_id=$1`, [id]); await client.query(`DELETE FROM assignments WHERE prayer_id=$1`, [id]); await client.query(`DELETE FROM prayers WHERE id=$1`, [id]); await client.query("COMMIT"); await broadcast(); res.json({ success: true, message: "Prayer permanently deleted." }); } catch (e) { try { await client.query("ROLLBACK"); } catch { } dbError(res, e); } finally { client.release(); } });

app.post("/api/room/join", async (req, res) => { try { const name = String(req.body.name || "").trim(); if (!name) return res.status(400).json({ error: "Name is required." }); const token = "room_" + crypto.randomUUID(); const r = await pool.query(`INSERT INTO participants(name,session_token,joined_at,last_seen) VALUES($1,$2,NOW(),NOW()) RETURNING id,name`, [name, token]); await assignWaiting(); await broadcast(); res.status(201).json({ participantId: r.rows[0].id, token, name: r.rows[0].name }); } catch (e) { dbError(res, e); } });
app.post("/api/room/heartbeat", async (req, res) => { try { const token = String(req.body.token || ""); const r = await pool.query(`UPDATE participants SET last_seen=NOW() WHERE session_token=$1 RETURNING id`, [token]); if (!r.rowCount) return res.status(404).json({ error: "Room participant not found." }); await assignWaiting(); res.json({ ok: true, roomCount: await emitRoom() }); } catch (e) { dbError(res, e); } });
app.post("/api/room/leave", async (req, res) => { try { await pool.query(`DELETE FROM participants WHERE session_token=$1`, [String(req.body.token || "")]); await broadcast(); res.json({ ok: true, roomCount: (await activePeople()).length }); } catch (e) { dbError(res, e); } });
app.get("/api/room/:token", async (req, res) => { try { const token = req.params.token, pr = (await pool.query(`SELECT id,name FROM participants WHERE session_token=$1`, [token])).rows[0]; if (!pr) return res.status(404).json({ error: "Prayer room participant not found." }); await pool.query(`UPDATE participants SET last_seen=NOW() WHERE id=$1`, [pr.id]); const rows = (await pool.query(`SELECT a.id assignment_id,a.prayer_id,a.completed_at,p.id,p.name,p.request,p.created_at,p.visibility,p.answered_at FROM assignments a JOIN prayers p ON p.id=a.prayer_id WHERE a.participant_id=$1 AND p.visibility='public' AND p.answered_at IS NULL ORDER BY a.assigned_at DESC`, [pr.id])).rows; const c = await reactionCounts(rows.map(x => x.id)); res.json({ participant: pr, roomCount: (await activePeople()).length, prayers: rows.map(x => ({ ...x, prayer_count: c[String(x.id)] || 0 })) }); } catch (e) { dbError(res, e); } });
app.post("/api/room/assignments/:id/pray", async (req, res) => { try { const aid = Number(req.params.id), token = String(req.body.token || ""), person = (await pool.query(`SELECT id FROM participants WHERE session_token=$1`, [token])).rows[0]; if (!person) return res.status(404).json({ error: "Participant not found." }); const a = (await pool.query(`SELECT id,prayer_id,completed_at FROM assignments WHERE id=$1 AND participant_id=$2`, [aid, person.id])).rows[0]; if (!a) return res.status(404).json({ error: "Prayer assignment not found." }); if (!a.completed_at) { const rt = token + "_prayer_" + a.prayer_id; try { await pool.query(`INSERT INTO prayer_reactions(prayer_id,participant_id,reactor_token) VALUES($1,$2,$3)`, [a.prayer_id, person.id, rt]); } catch (e) { if (e.code !== "23505") throw e; } await pool.query(`UPDATE assignments SET completed_at=NOW() WHERE id=$1 AND participant_id=$2`, [aid, person.id]); await assignWaiting(); await broadcast(); } const c = await pool.query(`SELECT COUNT(*)::int count FROM prayer_reactions WHERE prayer_id=$1`, [a.prayer_id]); res.json({ success: true, count: c.rows[0].count }); } catch (e) { dbError(res, e); } });

app.get("/admin", (req, res) => res.sendFile(path.join(__dirname, "public", "admin.html")));
io.on("connection", async () => { try { await emitRoom(); } catch (e) { console.error(e.message); } });
async function start() { try { await pool.query("SELECT NOW()"); await pool.query(`ALTER TABLE prayers ADD COLUMN IF NOT EXISTS answered_at TIMESTAMPTZ NULL`); await maintenance(); server.listen(PORT, () => console.log(`Prayer Room running at http://localhost:${PORT}`)); } catch (e) { console.error("Unable to start server:", e.message); process.exit(1); } }
start();
