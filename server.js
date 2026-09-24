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

// ============================================================
// SUPABASE POSTGRES CONNECTION
// ============================================================
// This version connects directly to your Supabase PostgreSQL
// database using the Pooler connection details.
//
// .env:
// DB_HOST=aws-0-ap-northeast-1.pooler.supabase.com
// DB_PORT=6543
// DB_NAME=postgres
// DB_USER=postgres.pjxtfvzricpbvffwloku
// DB_PASSWORD=YOUR_DATABASE_PASSWORD
//
// Do NOT put the database password in frontend/public files.
// ============================================================

if (
    !process.env.DB_HOST ||
    !process.env.DB_PORT ||
    !process.env.DB_NAME ||
    !process.env.DB_USER ||
    !process.env.DB_PASSWORD
) {
    console.error("Missing database settings in .env.");
    console.error("Required: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD");
    process.exit(1);
}

const pool = new Pool({
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT),
    database: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    ssl: {
        rejectUnauthorized: false
    },
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000
});

pool.on("error", (error) => {
    console.error("Unexpected PostgreSQL pool error:", error.message);
});

const ADMIN_USERNAME = process.env.ADMIN_USERNAME || "admin";
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || "change-this-password";

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const now = () => new Date().toISOString();

function handleDbError(res, error) {
    console.error(error);

    return res.status(500).json({
        error: "Database operation failed.",
        detail:
            process.env.NODE_ENV === "production"
                ? undefined
                : error.message
    });
}

// ============================================================
// DATABASE HELPERS
// ============================================================

async function getActiveParticipants() {
    const cutoff = new Date(Date.now() - 10_000).toISOString();

    const result = await pool.query(
        `
        SELECT id, name, session_token, joined_at, last_seen
        FROM participants
        WHERE last_seen >= $1
        ORDER BY joined_at ASC
        `,
        [cutoff]
    );

    return result.rows;
}

async function getPublicPrayers() {
    const prayersResult = await pool.query(
        `
        SELECT id, name, request, visibility, created_at
        FROM prayers
        WHERE visibility = 'public'
        ORDER BY created_at DESC
        `
    );

    const prayers = prayersResult.rows;

    if (!prayers.length) {
        return [];
    }

    const ids = prayers.map((prayer) => prayer.id);

    const reactionsResult = await pool.query(
        `
        SELECT prayer_id
        FROM prayer_reactions
        WHERE prayer_id = ANY($1::bigint[])
        `,
        [ids]
    );

    const counts = {};

    for (const reaction of reactionsResult.rows) {
        counts[reaction.prayer_id] =
            (counts[reaction.prayer_id] || 0) + 1;
    }

    return prayers.map((prayer) => ({
        ...prayer,
        prayer_count: counts[prayer.id] || 0
    }));
}

async function broadcast() {
    io.emit("prayer-data-changed");
    await broadcastRoomPresence(true);
}

// ============================================================
// PRAYER ASSIGNMENT
// ============================================================

async function assignWaitingPrayers() {
    const people = await getActiveParticipants();

    if (!people.length) {
        return;
    }

    const assignmentsResult = await pool.query(
        `
        SELECT prayer_id, participant_id, completed_at
        FROM assignments
        `
    );

    const assignments = assignmentsResult.rows;

    const assignedPrayerIds = new Set(
        assignments.map((assignment) => String(assignment.prayer_id))
    );

    const waitingResult = await pool.query(
        `
        SELECT id
        FROM prayers
        WHERE visibility = 'public'
        ORDER BY created_at ASC
        `
    );

    const waiting = waitingResult.rows;

    const unassigned = waiting.filter(
        (prayer) => !assignedPrayerIds.has(String(prayer.id))
    );

    if (!unassigned.length) {
        return;
    }

    const counts = new Map();

    for (const person of people) {
        counts.set(
            String(person.id),
            assignments.filter(
                (assignment) =>
                    String(assignment.participant_id) === String(person.id) &&
                    !assignment.completed_at
            ).length
        );
    }

    for (const prayer of unassigned) {
        people.sort(
            (a, b) =>
                (counts.get(String(a.id)) || 0) -
                (counts.get(String(b.id)) || 0)
        );

        const person = people[0];

        try {
            await pool.query(
                `
                INSERT INTO assignments (prayer_id, participant_id)
                VALUES ($1, $2)
                `,
                [prayer.id, person.id]
            );

            counts.set(
                String(person.id),
                (counts.get(String(person.id)) || 0) + 1
            );
        } catch (error) {
            // assignments.prayer_id is UNIQUE.
            // If another request assigned the same prayer first,
            // PostgreSQL returns 23505. That is safe to ignore.
            if (error.code !== "23505") {
                throw error;
            }
        }
    }
}

async function cleanupAndAssign() {
    try {
        await assignWaitingPrayers();

        const people = await getActiveParticipants();

        io.emit("room-count", people.length);
    } catch (error) {
        console.error("room maintenance:", error.message);
    }
}

let lastRoomSignature = "";

async function broadcastRoomPresence(force = false) {
    try {
        const people = await getActiveParticipants();
        const signature = people.map(p => String(p.id)).sort().join(",");

        if (force || signature !== lastRoomSignature) {
            lastRoomSignature = signature;
            io.emit("room-presence", {
                count: people.length,
                people: people.map(p => ({ id: p.id, name: p.name }))
            });
            io.emit("room-count", people.length);
        }
    } catch (error) {
        console.error("room presence:", error.message);
    }
}

setInterval(async () => {
    try {
        await assignWaitingPrayers();
        await broadcastRoomPresence();
    } catch (error) {
        console.error("room maintenance:", error.message);
    }
}, 2000);

// ============================================================
// PUBLIC API
// ============================================================

// Submit a prayer request.
app.post("/api/prayers", async (req, res) => {
    try {
        const name =
            String(req.body.name || "").trim() || null;

        const request =
            String(req.body.request || "").trim();

        const visibility =
            req.body.visibility === "private"
                ? "private"
                : "public";

        if (!request) {
            return res.status(400).json({
                error: "Prayer request is required."
            });
        }

        const result = await pool.query(
            `
            INSERT INTO prayers
                (name, request, visibility, created_at)
            VALUES
                ($1, $2, $3, $4)
            RETURNING id
            `,
            [name, request, visibility, now()]
        );

        const prayer = result.rows[0];

        if (visibility === "public") {
            await assignWaitingPrayers();
        }

        await broadcast();

        return res.status(201).json({
            id: prayer.id,
            message:
                visibility === "public"
                    ? "Your public prayer request was submitted."
                    : "Your private prayer request was submitted. Only administrators can see it."
        });
    } catch (error) {
        return handleDbError(res, error);
    }
});

// Get public prayers only.
app.get("/api/public/prayers", async (req, res) => {
    try {
        const prayers = await getPublicPrayers();

        return res.json(prayers);
    } catch (error) {
        return handleDbError(res, error);
    }
});

// Delete a prayer.
// This route intentionally has no admin authentication because the
// project requirement is that anyone can delete a prayer.
// Deleting a prayer is explicit; assigning it to the Prayer Room
// never deletes it. Related reactions and assignments are removed
// automatically by the database CASCADE rules.
app.delete("/api/prayers/:id", async (req, res) => {
    const prayerId = Number(req.params.id);

    if (!Number.isInteger(prayerId)) {
        return res.status(400).json({ error: "Invalid prayer ID." });
    }

    const client = await pool.connect();

    try {
        await client.query("BEGIN");

        const existing = await client.query(
            `SELECT id, visibility FROM prayers WHERE id = $1 FOR UPDATE`,
            [prayerId]
        );

        if (!existing.rows.length) {
            await client.query("ROLLBACK");
            return res.status(404).json({ error: "Prayer not found." });
        }

        await client.query(
            `DELETE FROM prayers WHERE id = $1`,
            [prayerId]
        );

        await client.query("COMMIT");

        io.emit("prayer-deleted", { prayerId });
        await broadcast();

        return res.json({
            success: true,
            message: "Prayer deleted successfully."
        });
    } catch (error) {
        try { await client.query("ROLLBACK"); } catch {}
        return handleDbError(res, error);
    } finally {
        client.release();
    }
});

// Public statistics.
app.get("/api/public/stats", async (req, res) => {
    try {
        const publicCountResult = await pool.query(
            `
            SELECT COUNT(*)::int AS count
            FROM prayers
            WHERE visibility = 'public'
            `
        );

        const prayedCountResult = await pool.query(
            `
            SELECT COUNT(*)::int AS count
            FROM prayer_reactions
            `
        );

        const roomCount =
            (await getActiveParticipants()).length;

        return res.json({
            prayerCount:
                publicCountResult.rows[0].count || 0,
            roomCount,
            prayedCount:
                prayedCountResult.rows[0].count || 0
        });
    } catch (error) {
        return handleDbError(res, error);
    }
});

// React/pray for a public prayer.
app.post("/api/prayers/:id/react", async (req, res) => {
    try {
        const prayerId = Number(req.params.id);
        const token =
            String(req.body.reactorToken || "").trim();

        if (!Number.isInteger(prayerId)) {
            return res.status(400).json({
                error: "Invalid prayer ID."
            });
        }

        if (!token) {
            return res.status(400).json({
                error: "Reactor token is required."
            });
        }

        const prayerResult = await pool.query(
            `
            SELECT id
            FROM prayers
            WHERE id = $1
              AND visibility = 'public'
            LIMIT 1
            `,
            [prayerId]
        );

        if (!prayerResult.rows.length) {
            return res.status(404).json({
                error: "Prayer not found."
            });
        }

        let participant = null;

        if (token.startsWith("room_")) {
            const participantResult = await pool.query(
                `
                SELECT id
                FROM participants
                WHERE session_token = $1
                LIMIT 1
                `,
                [token]
            );

            participant = participantResult.rows[0] || null;
        }

        let reacted = true;

        try {
            await pool.query(
                `
                INSERT INTO prayer_reactions
                    (prayer_id, participant_id, reactor_token, created_at)
                VALUES
                    ($1, $2, $3, $4)
                `,
                [
                    prayerId,
                    participant ? participant.id : null,
                    token,
                    now()
                ]
            );
        } catch (error) {
            if (error.code === "23505") {
                reacted = false;
            } else {
                throw error;
            }
        }

        const countResult = await pool.query(
            `
            SELECT COUNT(*)::int AS count
            FROM prayer_reactions
            WHERE prayer_id = $1
            `,
            [prayerId]
        );

        const count = countResult.rows[0].count || 0;

        if (reacted) {
            await broadcast();
        }

        return res.json({
            reacted,
            count
        });
    } catch (error) {
        return handleDbError(res, error);
    }
});

// ============================================================
// PRAYER ROOM
// ============================================================

// Join prayer room.
app.post("/api/room/join", async (req, res) => {
    try {
        const name =
            String(req.body.name || "").trim();

        if (!name) {
            return res.status(400).json({
                error: "Name is required."
            });
        }

        const token =
            "room_" + crypto.randomUUID();

        const result = await pool.query(
            `
            INSERT INTO participants
                (name, session_token, joined_at, last_seen)
            VALUES
                ($1, $2, $3, $4)
            RETURNING id, name
            `,
            [name, token, now(), now()]
        );

        const participant = result.rows[0];

        await assignWaitingPrayers();
        await broadcast();

        return res.status(201).json({
            participantId: participant.id,
            token,
            name: participant.name
        });
    } catch (error) {
        return handleDbError(res, error);
    }
});

// Keep participant active.
app.post("/api/room/heartbeat", async (req, res) => {
    try {
        const token =
            String(req.body.token || "").trim();

        if (!token) {
            return res.status(400).json({
                error: "Room token is required."
            });
        }

        const result = await pool.query(
            `
            UPDATE participants
            SET last_seen = $1
            WHERE session_token = $2
            RETURNING id
            `,
            [now(), token]
        );

        if (!result.rows.length) {
            return res.status(404).json({
                error: "Participant not found."
            });
        }

        await assignWaitingPrayers();

        return res.json({
            ok: true,
            roomCount:
                (await getActiveParticipants()).length
        });
    } catch (error) {
        return handleDbError(res, error);
    }
});

// Leave prayer room.
app.post("/api/room/leave", async (req, res) => {
    try {
        const token =
            String(req.body.token || "").trim();

        if (!token) {
            return res.status(400).json({
                error: "Room token is required."
            });
        }

        await pool.query(
            `
            UPDATE participants
            SET last_seen = $1
            WHERE session_token = $2
            `,
            [new Date(0).toISOString(), token]
        );

        await broadcast();

        return res.json({
            ok: true
        });
    } catch (error) {
        return handleDbError(res, error);
    }
});

// Get participant's assigned prayers.
app.get("/api/room/:token", async (req, res) => {
    try {
        const token = req.params.token;

        const participantResult = await pool.query(
            `
            SELECT id, name
            FROM participants
            WHERE session_token = $1
            LIMIT 1
            `,
            [token]
        );

        const participant =
            participantResult.rows[0];

        if (!participant) {
            return res.status(404).json({
                error: "Prayer room participant not found."
            });
        }

        await pool.query(
            `
            UPDATE participants
            SET last_seen = $1
            WHERE session_token = $2
            `,
            [now(), token]
        );

        const assignmentsResult = await pool.query(
            `
            SELECT id, prayer_id, completed_at
            FROM assignments
            WHERE participant_id = $1
            ORDER BY assigned_at DESC
            `,
            [participant.id]
        );

        const assignments =
            assignmentsResult.rows;

        const prayerIds =
            assignments.map(
                (assignment) => assignment.prayer_id
            );

        let prayers = [];

        if (prayerIds.length) {
            const prayersResult = await pool.query(
                `
                SELECT id, name, request, created_at, visibility
                FROM prayers
                WHERE id = ANY($1::bigint[])
                  AND visibility = 'public'
                `,
                [prayerIds]
            );

            const reactionsResult = await pool.query(
                `
                SELECT prayer_id
                FROM prayer_reactions
                WHERE prayer_id = ANY($1::bigint[])
                `,
                [prayerIds]
            );

            const counts = {};

            for (const reaction of reactionsResult.rows) {
                counts[reaction.prayer_id] =
                    (counts[reaction.prayer_id] || 0) + 1;
            }

            const byId = new Map(
                prayersResult.rows.map(
                    (prayer) => [String(prayer.id), prayer]
                )
            );

            prayers = assignments
                .map((assignment) => {
                    const prayer =
                        byId.get(
                            String(assignment.prayer_id)
                        );

                    if (!prayer) {
                        return null;
                    }

                    return {
                        assignment_id: assignment.id,
                        completed_at:
                            assignment.completed_at,
                        ...prayer,
                        prayer_count:
                            counts[assignment.prayer_id] || 0
                    };
                })
                .filter(Boolean);
        }

        return res.json({
            participant,
            roomCount:
                (await getActiveParticipants()).length,
            prayers
        });
    } catch (error) {
        return handleDbError(res, error);
    }
});

// Mark assigned prayer as prayed.
app.post("/api/room/assignments/:id/pray", async (req, res) => {
    try {
        const assignmentId =
            Number(req.params.id);

        const token =
            String(req.body.token || "").trim();

        if (!Number.isInteger(assignmentId)) {
            return res.status(400).json({
                error: "Invalid assignment ID."
            });
        }

        if (!token) {
            return res.status(400).json({
                error: "Room token is required."
            });
        }

        const participantResult = await pool.query(
            `
            SELECT id
            FROM participants
            WHERE session_token = $1
            LIMIT 1
            `,
            [token]
        );

        const participant =
            participantResult.rows[0];

        if (!participant) {
            return res.status(404).json({
                error: "Participant not found."
            });
        }

        const assignmentResult = await pool.query(
            `
            SELECT id, prayer_id, completed_at
            FROM assignments
            WHERE id = $1
              AND participant_id = $2
            LIMIT 1
            `,
            [assignmentId, participant.id]
        );

        const assignment =
            assignmentResult.rows[0];

        if (!assignment) {
            return res.status(404).json({
                error: "Prayer assignment not found."
            });
        }

        if (!assignment.completed_at) {
            const reactionToken =
                token +
                "_prayer_" +
                assignment.prayer_id;

            try {
                await pool.query(
                    `
                    INSERT INTO prayer_reactions
                        (prayer_id, participant_id, reactor_token, created_at)
                    VALUES
                        ($1, $2, $3, $4)
                    `,
                    [
                        assignment.prayer_id,
                        participant.id,
                        reactionToken,
                        now()
                    ]
                );
            } catch (error) {
                // Already prayed for this assignment.
                if (error.code !== "23505") {
                    throw error;
                }
            }

            await pool.query(
                `
                UPDATE assignments
                SET completed_at = $1
                WHERE id = $2
                  AND participant_id = $3
                `,
                [now(), assignmentId, participant.id]
            );

            await assignWaitingPrayers();
            await broadcast();
        }

        const countResult = await pool.query(
            `
            SELECT COUNT(*)::int AS count
            FROM prayer_reactions
            WHERE prayer_id = $1
            `,
            [assignment.prayer_id]
        );

        return res.json({
            success: true,
            count: countResult.rows[0].count || 0
        });
    } catch (error) {
        return handleDbError(res, error);
    }
});

// ============================================================
// ADMIN
// ============================================================

function adminAuth(req, res, next) {
    const header =
        req.headers.authorization || "";

    if (!header.startsWith("Basic ")) {
        res.setHeader(
            "WWW-Authenticate",
            'Basic realm="Prayer Admin"'
        );

        return res.status(401).json({
            error: "Admin authentication required."
        });
    }

    const decoded =
        Buffer.from(
            header.slice(6),
            "base64"
        ).toString();

    const separator =
        decoded.indexOf(":");

    const username =
        decoded.slice(0, separator);

    const password =
        decoded.slice(separator + 1);

    if (
        username !== ADMIN_USERNAME ||
        password !== ADMIN_PASSWORD
    ) {
        res.setHeader(
            "WWW-Authenticate",
            'Basic realm="Prayer Admin"'
        );

        return res.status(401).json({
            error: "Invalid admin credentials."
        });
    }

    next();
}

// Admin prayer list.
app.get("/api/admin/prayers", adminAuth, async (req, res) => {
    try {
        const visibility =
            String(req.query.visibility || "all");

        const sort =
            String(req.query.sort || "newest");

        const date =
            String(req.query.date || "");

        const search =
            String(req.query.search || "").trim();

        const params = [];
        const conditions = [];

        if (
            visibility === "public" ||
            visibility === "private"
        ) {
            params.push(visibility);
            conditions.push(
                `p.visibility = $${params.length}`
            );
        }

        if (date) {
            const start =
                new Date(`${date}T00:00:00.000Z`);

            if (!Number.isNaN(start.getTime())) {
                const end =
                    new Date(
                        start.getTime() + 86400000
                    );

                params.push(start.toISOString());
                const startParam = params.length;

                params.push(end.toISOString());
                const endParam = params.length;

                conditions.push(
                    `p.created_at >= $${startParam}
                     AND p.created_at < $${endParam}`
                );
            }
        }

        if (search) {
            params.push(`%${search}%`);

            const searchParam =
                params.length;

            conditions.push(
                `(p.request ILIKE $${searchParam}
                  OR COALESCE(p.name, '') ILIKE $${searchParam})`
            );
        }

        const whereClause =
            conditions.length
                ? `WHERE ${conditions.join(" AND ")}`
                : "";

        const result =
            await pool.query(
                `
                SELECT
                    p.id,
                    p.name,
                    p.request,
                    p.visibility,
                    p.created_at,
                    COUNT(r.id)::int AS prayer_count
                FROM prayers p
                LEFT JOIN prayer_reactions r
                    ON r.prayer_id = p.id
                ${whereClause}
                GROUP BY
                    p.id,
                    p.name,
                    p.request,
                    p.visibility,
                    p.created_at
                ORDER BY p.created_at DESC
                `,
                params
            );

        let rows = result.rows.map(
            (row) => ({
                ...row,
                prayer_count:
                    Number(row.prayer_count) || 0
            })
        );

        if (sort === "oldest") {
            rows.sort(
                (a, b) =>
                    new Date(a.created_at) -
                    new Date(b.created_at)
            );
        } else if (sort === "most_prayed") {
            rows.sort(
                (a, b) =>
                    b.prayer_count -
                        a.prayer_count ||
                    new Date(b.created_at) -
                        new Date(a.created_at)
            );
        } else {
            rows.sort(
                (a, b) =>
                    new Date(b.created_at) -
                    new Date(a.created_at)
            );
        }

        return res.json(rows);
    } catch (error) {
        return handleDbError(res, error);
    }
});

// Admin statistics.
app.get("/api/admin/stats", adminAuth, async (req, res) => {
    try {
        const result =
            await pool.query(
                `
                SELECT
                    COUNT(*)::int AS all_count,
                    COUNT(*) FILTER (
                        WHERE visibility = 'public'
                    )::int AS public_count,
                    COUNT(*) FILTER (
                        WHERE visibility = 'private'
                    )::int AS private_count
                FROM prayers
                `
            );

        const reactionResult =
            await pool.query(
                `
                SELECT COUNT(*)::int AS count
                FROM prayer_reactions
                `
            );

        const stats =
            result.rows[0];

        return res.json({
            all: stats.all_count || 0,
            public: stats.public_count || 0,
            private: stats.private_count || 0,
            reactions:
                reactionResult.rows[0].count || 0,
            room:
                (await getActiveParticipants()).length
        });
    } catch (error) {
        return handleDbError(res, error);
    }
});

// Admin participants.
app.get(
    "/api/admin/participants",
    adminAuth,
    async (req, res) => {
        try {
            return res.json(
                await getActiveParticipants()
            );
        } catch (error) {
            return handleDbError(res, error);
        }
    }
);

// Admin page.
app.get("/admin", (req, res) => {
    res.sendFile(
        path.join(
            __dirname,
            "public",
            "admin.html"
        )
    );
});

// ============================================================
// SOCKET.IO
// ============================================================

io.on("connection", async (socket) => {
    try {
        await broadcastRoomPresence(true);
    } catch (error) {
        console.error("Socket room presence:", error.message);
    }
});

// ============================================================
// START SERVER
// ============================================================

async function startServer() {
    try {
        // Test the database connection before starting
        // the HTTP server.
        const result =
            await pool.query("SELECT NOW() AS now");

        console.log(
            "PostgreSQL connected:",
            result.rows[0].now
        );

        server.listen(PORT, () => {
            console.log(
                `Prayer Room running at http://localhost:${PORT}`
            );
            console.log(
                `Admin: http://localhost:${PORT}/admin`
            );
        });
    } catch (error) {
        console.error(
            "Could not connect to PostgreSQL."
        );
        console.error(error.message);
        process.exit(1);
    }
}

startServer();
