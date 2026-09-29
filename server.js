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
const ACTIVE_MS = 10000;

const missing = [
    "DB_HOST",
    "DB_PORT",
    "DB_NAME",
    "DB_USER",
    "DB_PASSWORD"
].filter(k => !process.env[k]);

if (missing.length) {
    console.error(
        "Missing .env values:",
        missing.join(", ")
    );
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

const ADMIN_USERNAME =
    process.env.ADMIN_USERNAME || "admin";

const ADMIN_PASSWORD =
    process.env.ADMIN_PASSWORD ||
    "change-this-password";

app.use(express.json());
app.use(
    express.static(
        path.join(__dirname, "public")
    )
);

/* =========================================================
   GENERAL HELPERS
   ========================================================= */

function dbError(res, e) {
    console.error(e);

    return res.status(500).json({
        error: "Database operation failed.",
        detail:
            process.env.NODE_ENV === "production"
                ? undefined
                : e.message
    });
}

function adminAuth(req, res, next) {
    const h =
        req.headers.authorization || "";

    if (!h.startsWith("Basic ")) {
        res.setHeader(
            "WWW-Authenticate",
            'Basic realm="Prayer Admin"'
        );

        return res.status(401).json({
            error:
                "Admin authentication required."
        });
    }

    const decoded = Buffer.from(
        h.slice(6),
        "base64"
    ).toString("utf8");

    const i = decoded.indexOf(":");

    const username =
        i >= 0
            ? decoded.slice(0, i)
            : "";

    const password =
        i >= 0
            ? decoded.slice(i + 1)
            : "";

    if (
        username !== ADMIN_USERNAME ||
        password !== ADMIN_PASSWORD
    ) {
        res.setHeader(
            "WWW-Authenticate",
            'Basic realm="Prayer Admin"'
        );

        return res.status(401).json({
            error:
                "Invalid admin credentials."
        });
    }

    next();
}

function generateRoomCode() {
    return crypto
        .randomBytes(4)
        .toString("hex")
        .toUpperCase();
}

function isValidToken(token) {
    return (
        typeof token === "string" &&
        token.trim().length > 0
    );
}

function activeCutoff() {
    return new Date(
        Date.now() - ACTIVE_MS
    );
}

/* =========================================================
   PARTICIPANTS / ROOMS
   ========================================================= */

async function getParticipantByToken(token) {
    if (!isValidToken(token)) {
        return null;
    }

    const result = await pool.query(
        `
        SELECT
            p.id,
            p.name,
            p.session_token,
            p.joined_at,
            p.last_seen,
            p.room_id,
            r.code AS room_code,
            r.name AS room_name,
            r.is_private,
            r.owner_token
        FROM participants p
        LEFT JOIN rooms r
            ON r.id = p.room_id
        WHERE p.session_token = $1
        LIMIT 1
        `,
        [token]
    );

    return result.rows[0] || null;
}

async function activePeople() {
    const result = await pool.query(
        `
        SELECT
            id,
            name,
            session_token,
            joined_at,
            last_seen,
            room_id
        FROM participants
        WHERE last_seen >= $1
        ORDER BY joined_at
        `,
        [activeCutoff()]
    );

    return result.rows;
}

async function activePeopleForRoom(roomId) {
    if (roomId) {
        const result = await pool.query(
            `
            SELECT
                id,
                name,
                joined_at,
                last_seen
            FROM participants
            WHERE room_id = $1
                AND last_seen >= $2
            ORDER BY joined_at
            `,
            [
                roomId,
                activeCutoff()
            ]
        );

        return result.rows;
    }

    const result = await pool.query(
        `
        SELECT
            id,
            name,
            joined_at,
            last_seen
        FROM participants
        WHERE room_id IS NULL
            AND last_seen >= $1
        ORDER BY joined_at
        `,
        [activeCutoff()]
    );

    return result.rows;
}

async function emitRoom() {
    try {
        const publicPeople =
            await activePeopleForRoom(null);

        io.emit(
            "room-count",
            publicPeople.length
        );

        io.emit(
            "room-participants-changed",
            publicPeople.map(
                ({
                    id,
                    name,
                    joined_at
                }) => ({
                    id,
                    name,
                    joined_at
                })
            )
        );

        return publicPeople.length;
    } catch (e) {
        console.error(
            "emitRoom:",
            e.message
        );

        return 0;
    }
}

function emitData() {
    io.emit(
        "prayer-data-changed"
    );
}

function emitRoomDataChanged() {
    io.emit(
        "room-data-changed"
    );
}

async function broadcast() {
    emitData();
    emitRoomDataChanged();

    try {
        await emitRoom();
    } catch (e) {
        console.error(
            "room broadcast:",
            e.message
        );
    }
}

/* =========================================================
   PRAYER HELPERS
   ========================================================= */

async function reactionCounts(ids) {
    if (!ids.length) {
        return {};
    }

    const result = await pool.query(
        `
        SELECT
            prayer_id,
            COUNT(*)::int AS count
        FROM prayer_reactions
        WHERE prayer_id =
            ANY($1::bigint[])
        GROUP BY prayer_id
        `,
        [ids]
    );

    return Object.fromEntries(
        result.rows.map(row => [
            String(row.prayer_id),
            row.count
        ])
    );
}

async function roomPrayerReactionCounts(ids) {
    if (!ids.length) {
        return {};
    }

    const result = await pool.query(
        `
        SELECT
            room_prayer_id,
            COUNT(*)::int AS count
        FROM room_prayer_reactions
        WHERE room_prayer_id =
            ANY($1::bigint[])
        GROUP BY room_prayer_id
        `,
        [ids]
    );

    return Object.fromEntries(
        result.rows.map(row => [
            String(row.room_prayer_id),
            row.count
        ])
    );
}

async function publicPrayers() {
    const result = await pool.query(
        `
        SELECT
            id,
            name,
            request,
            visibility,
            created_at,
            answered_at
        FROM prayers
        WHERE visibility = 'public'
        ORDER BY
            CASE
                WHEN answered_at IS NULL
                    THEN 0
                ELSE 1
            END,
            created_at DESC
        `
    );

    const counts =
        await reactionCounts(
            result.rows.map(
                row => row.id
            )
        );

    return result.rows.map(row => ({
        ...row,
        prayer_count:
            counts[
            String(row.id)
            ] || 0
    }));
}

/* =========================================================
   ASSIGNMENT / MAINTENANCE
   ========================================================= */

/*
    IMPORTANT:

    There is NO automatic assignment anymore.

    A prayer is assigned only when a participant
    explicitly calls:

        POST /api/room/assign

    When a participant becomes inactive,
    their unfinished assignments are deleted.
*/

async function syncParticipantPrayers(
    token,
    amount,
    mode,
    keyword
) {
    const participant =
        await getParticipantByToken(token);

    if (!participant) {
        throw new Error(
            "Prayer room participant not found."
        );
    }

    // Keep participant active
    await pool.query(
        `
        UPDATE participants
        SET last_seen = NOW()
        WHERE id = $1
        `,
        [participant.id]
    );

    await releaseStaleAssignments();

    amount = Math.min(
        Math.max(
            Number(amount) || 1,
            1
        ),
        20
    );

    const validModes = [
        "random",
        "newest",
        "oldest",
        "least_prayed",
        "most_prayed"
    ];

    if (!validModes.includes(mode)) {
        mode = "random";
    }

    keyword =
        String(keyword || "").trim();

    /*
     * If the user changed the keyword,
     * release their current assignments
     * that no longer match.
     */
    if (keyword) {

        await pool.query(
            `
            DELETE FROM assignments a
            USING prayers p
            WHERE a.prayer_id = p.id
                AND a.participant_id = $1
                AND a.completed_at IS NULL
                AND NOT (
                    p.request ILIKE $2
                    OR COALESCE(
                        p.name,
                        ''
                    ) ILIKE $2
                )
            `,
            [
                participant.id,
                `%${keyword}%`
            ]
        );

    }

    /*
     * Remove assignments for prayers that
     * are no longer available.
     */
    await pool.query(
        `
        DELETE FROM assignments a
        USING prayers p
        WHERE a.participant_id = $1
            AND a.completed_at IS NULL
            AND (
                p.visibility <> 'public'
                OR p.answered_at IS NOT NULL
            )
        `,
        [participant.id]
    );

    /*
     * Check how many prayers this person
     * already has.
     */
    const current =
        await pool.query(
            `
            SELECT
                a.id AS assignment_id,
                a.prayer_id,
                a.assigned_at,
                a.completed_at,
                p.id,
                p.name,
                p.request,
                p.created_at,
                p.visibility,
                p.answered_at
            FROM assignments a
            JOIN prayers p
                ON p.id = a.prayer_id
            WHERE a.participant_id = $1
                AND a.completed_at IS NULL
                AND p.visibility = 'public'
                AND p.answered_at IS NULL
            ORDER BY a.assigned_at DESC
            `,
            [participant.id]
        );

    /*
     * If the requested amount became smaller,
     * release the extra assignments.
     */
    if (
        current.rows.length > amount
    ) {

        const extras =
            current.rows
                .slice(amount);

        for (
            const assignment
            of extras
        ) {

            await pool.query(
                `
                DELETE FROM assignments
                WHERE id = $1
                    AND participant_id = $2
                    AND completed_at IS NULL
                `,
                [
                    assignment.assignment_id,
                    participant.id
                ]
            );

        }

    }

    /*
     * Get current assignments again.
     */
    const refreshed =
        await pool.query(
            `
            SELECT
                a.id AS assignment_id,
                a.prayer_id,
                a.assigned_at,
                a.completed_at,
                p.id,
                p.name,
                p.request,
                p.created_at,
                p.visibility,
                p.answered_at
            FROM assignments a
            JOIN prayers p
                ON p.id = a.prayer_id
            WHERE a.participant_id = $1
                AND a.completed_at IS NULL
                AND p.visibility = 'public'
                AND p.answered_at IS NULL
            ORDER BY a.assigned_at DESC
            `,
            [participant.id]
        );

    let needed =
        amount -
        refreshed.rows.length;

    if (needed <= 0) {

        const counts =
            await reactionCounts(
                refreshed.rows.map(
                    row => row.prayer_id
                )
            );

        return refreshed.rows.map(
            row => ({
                ...row,

                prayer_count:
                    counts[
                    String(
                        row.prayer_id
                    )
                    ] || 0
            })
        );

    }

    /*
     * Build selection order.
     */
    let orderBy;

    switch (mode) {

        case "newest":

            orderBy =
                "p.created_at DESC";

            break;

        case "oldest":

            orderBy =
                "p.created_at ASC";

            break;

        case "least_prayed":

            orderBy = `
                (
                    SELECT COUNT(*)
                    FROM prayer_reactions pr
                    WHERE pr.prayer_id = p.id
                ) ASC,
                p.created_at DESC
            `;

            break;

        case "most_prayed":

            orderBy = `
                (
                    SELECT COUNT(*)
                    FROM prayer_reactions pr
                    WHERE pr.prayer_id = p.id
                ) DESC,
                p.created_at DESC
            `;

            break;

        case "random":
        default:

            orderBy =
                "RANDOM()";

            break;
    }

    const params = [
        needed
    ];

    let keywordCondition = "";

    if (keyword) {

        params.push(
            `%${keyword}%`
        );

        const keywordParam =
            params.length;

        keywordCondition = `
            AND (
                p.request ILIKE
                    $${keywordParam}

                OR COALESCE(
                    p.name,
                    ''
                ) ILIKE
                    $${keywordParam}
            )
        `;
    }

    /*
     * Only choose prayers that are not
     * currently assigned to somebody.
     */
    const available =
        await pool.query(
            `
            SELECT
                p.id,
                p.name,
                p.request,
                p.visibility,
                p.created_at,
                p.answered_at

            FROM prayers p

            WHERE p.visibility =
                'public'

                AND p.answered_at
                    IS NULL

                AND NOT EXISTS (
                    SELECT 1
                    FROM assignments a
                    WHERE a.prayer_id =
                        p.id

                        AND a.completed_at
                            IS NULL
                )

                ${keywordCondition}

            ORDER BY ${orderBy}

            LIMIT $1

            FOR UPDATE OF p
            SKIP LOCKED
            `,
            params
        );

    /*
     * Create assignments.
     */
    for (
        const prayer
        of available.rows
    ) {

        try {

            await pool.query(
                `
                INSERT INTO assignments(
                    prayer_id,
                    participant_id,
                    assigned_at,
                    completed_at
                )
                VALUES(
                    $1,
                    $2,
                    NOW(),
                    NULL
                )
                `,
                [
                    prayer.id,
                    participant.id
                ]
            );

        } catch (e) {

            /*
             * Another participant may have
             * taken this prayer at the same time.
             */
            if (
                e.code !== "23505"
            ) {
                throw e;
            }

        }

    }

    /*
     * Return final assignments.
     */
    const finalResult =
        await pool.query(
            `
            SELECT
                a.id AS assignment_id,
                a.prayer_id,
                a.assigned_at,
                a.completed_at,
                p.id,
                p.name,
                p.request,
                p.created_at,
                p.visibility,
                p.answered_at
            FROM assignments a
            JOIN prayers p
                ON p.id = a.prayer_id
            WHERE a.participant_id = $1
                AND a.completed_at IS NULL
                AND p.visibility = 'public'
                AND p.answered_at IS NULL
            ORDER BY a.assigned_at DESC
            `,
            [participant.id]
        );

    const counts =
        await reactionCounts(
            finalResult.rows.map(
                row => row.prayer_id
            )
        );

    return finalResult.rows.map(
        row => ({
            ...row,

            prayer_count:
                counts[
                String(
                    row.prayer_id
                )
                ] || 0
        })
    );
}
app.post(
    "/api/room/sync",
    async (req, res) => {

        try {

            const token =
                String(
                    req.body.token || ""
                ).trim();


            const amount =
                Number(
                    req.body.amount || 5
                );


            const mode =
                String(
                    req.body.mode || "random"
                );


            const keyword =
                String(
                    req.body.keyword || ""
                ).trim();


            if (!token) {

                return res.status(400).json({
                    error:
                        "Room token is required."
                });

            }


            const prayers =
                await syncParticipantPrayers(
                    token,
                    amount,
                    mode,
                    keyword
                );


            await broadcast();


            return res.json({

                success: true,

                prayers,

                count:
                    prayers.length,

                message:
                    prayers.length
                        ? `${prayers.length} prayer request${prayers.length === 1 ? "" : "s"} found.`
                        : "No available prayer requests match your selection."

            });

        }

        catch (error) {

            return dbError(
                res,
                error
            );

        }

    }
);

async function releaseStaleAssignments() {
    await pool.query(
        `
        DELETE FROM assignments a
        USING participants p
        WHERE a.participant_id = p.id
            AND a.completed_at IS NULL
            AND p.last_seen < $1
        `,
        [activeCutoff()]
    );
}

async function cleanupTemporaryRoomPrayers() {
    await pool.query(
        `
        DELETE FROM room_prayers rp
        WHERE rp.is_temporary = TRUE
            AND NOT EXISTS (
                SELECT 1
                FROM participants p
                WHERE p.room_id =
                    rp.room_id
                    AND p.last_seen >= $1
            )
        `,
        [activeCutoff()]
    );
}

async function maintenance() {
    try {
        await releaseStaleAssignments();
        await cleanupTemporaryRoomPrayers();
        await emitRoom();
    } catch (e) {
        console.error(
            "maintenance:",
            e.message
        );
    }
}

setInterval(
    maintenance,
    2000
);

/* =========================================================
   PUBLIC PRAYER API
   ========================================================= */

app.post(
    "/api/prayers",
    async (req, res) => {
        try {
            const name =
                String(
                    req.body.name || ""
                ).trim() || null;

            const request =
                String(
                    req.body.request || ""
                ).trim();

            const visibility =
                req.body.visibility ===
                    "private"
                    ? "private"
                    : "public";

            if (!request) {
                return res.status(400).json({
                    error:
                        "Prayer request is required."
                });
            }

            const result =
                await pool.query(
                    `
                    INSERT INTO prayers(
                        name,
                        request,
                        visibility
                    )
                    VALUES($1, $2, $3)
                    RETURNING id
                    `,
                    [
                        name,
                        request,
                        visibility
                    ]
                );

            /*
                No automatic assignment.
            */

            await broadcast();

            res.status(201).json({
                id:
                    result.rows[0].id,

                message:
                    visibility ===
                        "public"
                        ? "Your public prayer request was submitted."
                        : "Your private prayer request was submitted. Only administrators can see it."
            });
        } catch (e) {
            dbError(res, e);
        }
    }
);

app.get(
    "/api/public/prayers",
    async (req, res) => {
        try {
            res.json(
                await publicPrayers()
            );
        } catch (e) {
            dbError(res, e);
        }
    }
);

app.get(
    "/api/public/stats",
    async (req, res) => {
        try {
            const [
                active,
                answered,
                prayed,
                room
            ] = await Promise.all([
                pool.query(`
                    SELECT
                        COUNT(*)::int AS count
                    FROM prayers
                    WHERE visibility = 'public'
                        AND answered_at IS NULL
                `),

                pool.query(`
                    SELECT
                        COUNT(*)::int AS count
                    FROM prayers
                    WHERE visibility = 'public'
                        AND answered_at IS NOT NULL
                `),

                pool.query(`
                    SELECT
                        COUNT(*)::int AS count
                    FROM prayer_reactions
                `),

                activePeopleForRoom(
                    null
                )
            ]);

            res.json({
                activePrayers:
                    active.rows[0].count,

                answeredPrayers:
                    answered.rows[0].count,

                prayedCount:
                    prayed.rows[0].count,

                roomCount:
                    room.length
            });
        } catch (e) {
            dbError(res, e);
        }
    }
);

app.post(
    "/api/prayers/:id/react",
    async (req, res) => {
        try {
            const id =
                Number(
                    req.params.id
                );

            const token =
                String(
                    req.body
                        .reactorToken || ""
                ).trim();

            if (
                !Number.isInteger(id) ||
                id <= 0 ||
                !token
            ) {
                return res.status(400).json({
                    error:
                        "Valid prayer ID and reactor token are required."
                });
            }

            const prayer =
                await pool.query(
                    `
                    SELECT id
                    FROM prayers
                    WHERE id = $1
                        AND visibility = 'public'
                    `,
                    [id]
                );

            if (!prayer.rowCount) {
                return res.status(404).json({
                    error:
                        "Prayer not found."
                });
            }

            let participantId =
                null;

            if (
                token.startsWith(
                    "room_"
                )
            ) {
                participantId =
                    (
                        await pool.query(
                            `
                            SELECT id
                            FROM participants
                            WHERE session_token = $1
                            `,
                            [token]
                        )
                    ).rows[0]?.id ||
                    null;
            }

            let reacted = true;

            try {
                await pool.query(
                    `
                    INSERT INTO prayer_reactions(
                        prayer_id,
                        participant_id,
                        reactor_token
                    )
                    VALUES($1, $2, $3)
                    `,
                    [
                        id,
                        participantId,
                        token
                    ]
                );
            } catch (e) {
                if (
                    e.code === "23505"
                ) {
                    reacted = false;
                } else {
                    throw e;
                }
            }

            const count =
                await pool.query(
                    `
                    SELECT
                        COUNT(*)::int AS count
                    FROM prayer_reactions
                    WHERE prayer_id = $1
                    `,
                    [id]
                );

            if (reacted) {
                await broadcast();
            }

            res.json({
                reacted,
                count:
                    count.rows[0]
                        .count
            });
        } catch (e) {
            dbError(res, e);
        }
    }
);

app.post(
    "/api/prayers/:id/answered",
    async (req, res) => {
        try {
            const id =
                Number(
                    req.params.id
                );

            if (
                !Number.isInteger(id) ||
                id <= 0
            ) {
                return res.status(400).json({
                    error:
                        "Invalid prayer ID."
                });
            }

            const result =
                await pool.query(
                    `
                    UPDATE prayers
                    SET answered_at =
                        COALESCE(
                            answered_at,
                            NOW()
                        )
                    WHERE id = $1
                        AND visibility = 'public'
                    RETURNING
                        id,
                        answered_at
                    `,
                    [id]
                );

            if (!result.rowCount) {
                return res.status(404).json({
                    error:
                        "Public prayer not found."
                });
            }

            await broadcast();

            res.json({
                success: true,
                answered_at:
                    result.rows[0]
                        .answered_at
            });
        } catch (e) {
            dbError(res, e);
        }
    }
);

/* =========================================================
   ADMIN API
   ========================================================= */

app.get(
    "/api/admin/prayers",
    adminAuth,
    async (req, res) => {
        try {
            const visibilityValues = [
                "all",
                "public",
                "private"
            ];

            const answeredValues = [
                "all",
                "active",
                "answered"
            ];

            const sortValues = [
                "newest",
                "oldest",
                "most_prayed",
                "least_prayed"
            ];

            const visibility =
                visibilityValues.includes(
                    String(
                        req.query.visibility ||
                        "all"
                    )
                )
                    ? String(
                        req.query
                            .visibility ||
                        "all"
                    )
                    : "all";

            const answered =
                answeredValues.includes(
                    String(
                        req.query.answered ||
                        "all"
                    )
                )
                    ? String(
                        req.query
                            .answered ||
                        "all"
                    )
                    : "all";

            const sort =
                sortValues.includes(
                    String(
                        req.query.sort ||
                        "newest"
                    )
                )
                    ? String(
                        req.query.sort ||
                        "newest"
                    )
                    : "newest";

            const date =
                String(
                    req.query.date || ""
                ).trim();

            const search =
                String(
                    req.query.search || ""
                ).trim();

            const where = [];
            const params = [];

            if (
                visibility !== "all"
            ) {
                params.push(
                    visibility
                );

                where.push(
                    `p.visibility = $${params.length}`
                );
            }

            if (
                answered === "active"
            ) {
                where.push(
                    `p.answered_at IS NULL`
                );
            }

            if (
                answered === "answered"
            ) {
                where.push(
                    `p.answered_at IS NOT NULL`
                );
            }

            if (date) {
                params.push(date);

                const dateParam =
                    params.length;

                where.push(
                    `p.created_at >= $${dateParam}::date
                    AND p.created_at <
                    ($${dateParam}::date +
                    INTERVAL '1 day')`
                );
            }

            if (search) {
                params.push(
                    `%${search}%`
                );

                const searchParam =
                    params.length;

                where.push(
                    `(p.request ILIKE $${searchParam}
                    OR COALESCE(p.name, '')
                    ILIKE $${searchParam})`
                );
            }

            let orderBy =
                "p.created_at DESC";

            if (sort === "oldest") {
                orderBy =
                    "p.created_at ASC";
            }

            if (
                sort === "most_prayed"
            ) {
                orderBy =
                    "prayer_count DESC, p.created_at DESC";
            }

            if (
                sort === "least_prayed"
            ) {
                orderBy =
                    "prayer_count ASC, p.created_at DESC";
            }

            const sql = `
                SELECT
                    p.id,
                    p.name,
                    p.request,
                    p.visibility,
                    p.created_at,
                    p.answered_at,
                    COUNT(pr.id)::int
                        AS prayer_count
                FROM prayers p
                LEFT JOIN prayer_reactions pr
                    ON pr.prayer_id = p.id
                ${where.length
                    ? "WHERE " +
                    where.join(
                        " AND "
                    )
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

            res.json(
                result.rows
            );
        } catch (e) {
            dbError(res, e);
        }
    }
);

app.get(
    "/api/admin/stats",
    adminAuth,
    async (req, res) => {
        try {
            const result =
                await pool.query(`
                SELECT
                    COUNT(*)::int
                        AS all_count,

                    COUNT(*) FILTER(
                        WHERE visibility =
                            'public'
                    )::int
                        AS public_count,

                    COUNT(*) FILTER(
                        WHERE visibility =
                            'private'
                    )::int
                        AS private_count,

                    COUNT(*) FILTER(
                        WHERE answered_at
                            IS NOT NULL
                    )::int
                        AS answered_count,

                    COUNT(*) FILTER(
                        WHERE visibility =
                            'public'
                        AND answered_at
                            IS NULL
                    )::int
                        AS active_count

                FROM prayers
            `);

            const reactions =
                await pool.query(`
                SELECT
                    COUNT(*)::int AS count
                FROM prayer_reactions
            `);

            const room =
                await activePeopleForRoom(
                    null
                );

            res.json({
                all:
                    result.rows[0]
                        .all_count,

                public:
                    result.rows[0]
                        .public_count,

                private:
                    result.rows[0]
                        .private_count,

                answered:
                    result.rows[0]
                        .answered_count,

                active:
                    result.rows[0]
                        .active_count,

                reactions:
                    reactions.rows[0]
                        .count,

                room:
                    room.length
            });
        } catch (e) {
            dbError(res, e);
        }
    }
);

app.get(
    "/api/admin/participants",
    adminAuth,
    async (req, res) => {
        try {
            res.json(
                await activePeople()
            );
        } catch (e) {
            dbError(res, e);
        }
    }
);

app.post(
    "/api/admin/prayers/:id/toggle-answered",
    adminAuth,
    async (req, res) => {
        try {
            const id =
                Number(
                    req.params.id
                );

            const current =
                await pool.query(
                    `
                    SELECT
                        id,
                        answered_at
                    FROM prayers
                    WHERE id = $1
                    `,
                    [id]
                );

            if (!current.rowCount) {
                return res.status(404).json({
                    error:
                        "Prayer not found."
                });
            }

            const next =
                current.rows[0]
                    .answered_at
                    ? null
                    : new Date();

            await pool.query(
                `
                UPDATE prayers
                SET answered_at = $1
                WHERE id = $2
                `,
                [
                    next,
                    id
                ]
            );

            await broadcast();

            res.json({
                success: true,
                answered:
                    Boolean(next)
            });
        } catch (e) {
            dbError(res, e);
        }
    }
);

app.delete(
    "/api/admin/prayers/:id",
    adminAuth,
    async (req, res) => {
        const client =
            await pool.connect();

        try {
            const id =
                Number(
                    req.params.id
                );

            if (
                !Number.isInteger(id) ||
                id <= 0
            ) {
                return res.status(400).json({
                    error:
                        "Invalid prayer ID."
                });
            }

            await client.query(
                "BEGIN"
            );

            const prayer =
                await client.query(
                    `
                    SELECT id
                    FROM prayers
                    WHERE id = $1
                    FOR UPDATE
                    `,
                    [id]
                );

            if (!prayer.rowCount) {
                await client.query(
                    "ROLLBACK"
                );

                return res.status(404).json({
                    error:
                        "Prayer not found."
                });
            }

            await client.query(
                `
                DELETE FROM prayer_reactions
                WHERE prayer_id = $1
                `,
                [id]
            );

            await client.query(
                `
                DELETE FROM assignments
                WHERE prayer_id = $1
                `,
                [id]
            );

            await client.query(
                `
                DELETE FROM prayers
                WHERE id = $1
                `,
                [id]
            );

            await client.query(
                "COMMIT"
            );

            await broadcast();

            res.json({
                success: true,
                message:
                    "Prayer permanently deleted."
            });
        } catch (e) {
            try {
                await client.query(
                    "ROLLBACK"
                );
            } catch { }

            dbError(res, e);
        } finally {
            client.release();
        }
    }
);

/* =========================================================
   PUBLIC ROOM / PRIVATE ROOM
   ========================================================= */

/*
    JOIN PUBLIC ROOM

    Blank roomCode:
        Public Prayer Room

    With roomCode:
        Private Prayer Room
*/

app.post(
    "/api/room/join",
    async (req, res) => {
        const client =
            await pool.connect();

        try {
            const name =
                String(
                    req.body.name || ""
                ).trim();

            const roomCode =
                String(
                    req.body.roomCode || ""
                )
                    .trim()
                    .toUpperCase();

            if (!name) {
                return res.status(400).json({
                    error:
                        "Name is required."
                });
            }

            if (name.length > 100) {
                return res.status(400).json({
                    error:
                        "Name must be 100 characters or less."
                });
            }

            let room = null;

            if (roomCode) {
                const roomResult =
                    await client.query(
                        `
                        SELECT
                            id,
                            code,
                            name,
                            is_private
                        FROM rooms
                        WHERE code = $1
                            AND is_private = TRUE
                        LIMIT 1
                        `,
                        [roomCode]
                    );

                if (!roomResult.rowCount) {
                    return res.status(404).json({
                        error:
                            "Private room code not found."
                    });
                }

                room =
                    roomResult.rows[0];
            }

            const token =
                "room_" +
                crypto.randomUUID();

            await client.query(
                "BEGIN"
            );

            const participant =
                await client.query(
                    `
                    INSERT INTO participants(
                        name,
                        session_token,
                        joined_at,
                        last_seen,
                        room_id
                    )
                    VALUES(
                        $1,
                        $2,
                        NOW(),
                        NOW(),
                        $3
                    )
                    RETURNING id, name
                    `,
                    [
                        name,
                        token,
                        room
                            ? room.id
                            : null
                    ]
                );

            await client.query(
                "COMMIT"
            );

            await broadcast();

            res.status(201).json({
                participantId:
                    participant.rows[0]
                        .id,

                token,

                name:
                    participant.rows[0]
                        .name,

                room: room
                    ? {
                        id:
                            room.id,
                        code:
                            room.code,
                        name:
                            room.name,
                        is_private:
                            true
                    }
                    : {
                        id: null,
                        code: null,
                        name:
                            "Public Prayer Room",
                        is_private:
                            false
                    }
            });
        } catch (e) {
            try {
                await client.query(
                    "ROLLBACK"
                );
            } catch { }

            dbError(res, e);
        } finally {
            client.release();
        }
    }
);

/*
    CREATE PRIVATE ROOM
*/

app.post(
    "/api/room/create",
    async (req, res) => {
        const client =
            await pool.connect();

        try {
            const name =
                String(
                    req.body.name || ""
                ).trim();

            const roomName =
                String(
                    req.body.roomName ||
                    "Private Prayer Room"
                ).trim();

            if (!name) {
                return res.status(400).json({
                    error:
                        "Your name is required."
                });
            }

            if (name.length > 100) {
                return res.status(400).json({
                    error:
                        "Your name must be 100 characters or less."
                });
            }

            if (roomName.length > 100) {
                return res.status(400).json({
                    error:
                        "Room name must be 100 characters or less."
                });
            }

            const token =
                "room_" +
                crypto.randomUUID();

            const roomId =
                crypto.randomUUID();

            let code = null;

            for (
                let attempt = 0;
                attempt < 10;
                attempt++
            ) {
                const candidate =
                    generateRoomCode();

                const existing =
                    await client.query(
                        `
                        SELECT id
                        FROM rooms
                        WHERE code = $1
                        LIMIT 1
                        `,
                        [candidate]
                    );

                if (
                    !existing.rowCount
                ) {
                    code =
                        candidate;

                    break;
                }
            }

            if (!code) {
                return res.status(500).json({
                    error:
                        "Could not generate a unique room code."
                });
            }

            await client.query(
                "BEGIN"
            );

            await client.query(
                `
                INSERT INTO rooms(
                    id,
                    code,
                    name,
                    is_private,
                    owner_token
                )
                VALUES(
                    $1,
                    $2,
                    $3,
                    TRUE,
                    $4
                )
                `,
                [
                    roomId,
                    code,
                    roomName ||
                    "Private Prayer Room",
                    token
                ]
            );

            const participant =
                await client.query(
                    `
                    INSERT INTO participants(
                        name,
                        session_token,
                        joined_at,
                        last_seen,
                        room_id
                    )
                    VALUES(
                        $1,
                        $2,
                        NOW(),
                        NOW(),
                        $3
                    )
                    RETURNING id, name
                    `,
                    [
                        name,
                        token,
                        roomId
                    ]
                );

            await client.query(
                "COMMIT"
            );

            await broadcast();

            res.status(201).json({
                participantId:
                    participant.rows[0]
                        .id,

                token,

                name:
                    participant.rows[0]
                        .name,

                room: {
                    id: roomId,
                    code,
                    name:
                        roomName ||
                        "Private Prayer Room",
                    is_private:
                        true,
                    owner: true
                }
            });
        } catch (e) {
            try {
                await client.query(
                    "ROLLBACK"
                );
            } catch { }

            dbError(res, e);
        } finally {
            client.release();
        }
    }
);

/*
    HEARTBEAT
*/

app.post(
    "/api/room/heartbeat",
    async (req, res) => {
        try {
            const token =
                String(
                    req.body.token || ""
                ).trim();

            if (
                !isValidToken(token)
            ) {
                return res.status(400).json({
                    error:
                        "Room token is required."
                });
            }

            const result =
                await pool.query(
                    `
                    UPDATE participants
                    SET last_seen = NOW()
                    WHERE session_token = $1
                    RETURNING id, room_id
                    `,
                    [token]
                );

            if (!result.rowCount) {
                return res.status(404).json({
                    error:
                        "Room participant not found."
                });
            }

            const roomId =
                result.rows[0]
                    .room_id;

            const people =
                await activePeopleForRoom(
                    roomId
                );

            res.json({
                ok: true,
                roomCount:
                    people.length
            });
        } catch (e) {
            dbError(res, e);
        }
    }
);

/*
    LEAVE ROOM
*/

app.post(
    "/api/room/leave",
    async (req, res) => {
        try {
            const token =
                String(
                    req.body.token || ""
                ).trim();

            if (
                !isValidToken(token)
            ) {
                return res.status(400).json({
                    error:
                        "Room token is required."
                });
            }

            const participant =
                await getParticipantByToken(
                    token
                );

            if (!participant) {
                return res.json({
                    ok: true,
                    roomCount: 0
                });
            }

            const roomId =
                participant.room_id;

            await pool.query(
                `
                DELETE FROM participants
                WHERE session_token = $1
                `,
                [token]
            );

            await broadcast();

            const people =
                await activePeopleForRoom(
                    roomId
                );

            res.json({
                ok: true,
                roomCount:
                    people.length
            });
        } catch (e) {
            dbError(res, e);
        }
    }
);

/* =========================================================
   GET CURRENT ROOM DATA
   ========================================================= */

app.get(
    "/api/room/:token",
    async (req, res) => {
        try {
            const token =
                req.params.token;

            const participant =
                await getParticipantByToken(
                    token
                );

            if (!participant) {
                return res.status(404).json({
                    error:
                        "Prayer room participant not found."
                });
            }

            await pool.query(
                `
                UPDATE participants
                SET last_seen = NOW()
                WHERE id = $1
                `,
                [participant.id]
            );

            const people =
                await activePeopleForRoom(
                    participant.room_id
                );

            /*
                Active assignments.
            */

            const activeAssignments =
                await pool.query(
                    `
                    SELECT
                        a.id AS assignment_id,
                        a.prayer_id,
                        a.assigned_at,
                        a.completed_at,
                        p.id,
                        p.name,
                        p.request,
                        p.created_at,
                        p.visibility,
                        p.answered_at
                    FROM assignments a
                    JOIN prayers p
                        ON p.id =
                            a.prayer_id
                    WHERE a.participant_id =
                        $1
                        AND a.completed_at
                            IS NULL
                        AND p.visibility =
                            'public'
                        AND p.answered_at
                            IS NULL
                    ORDER BY
                        a.assigned_at DESC
                    `,
                    [participant.id]
                );

            /*
                Completed assignments.
            */

            const completedAssignments =
                await pool.query(
                    `
                    SELECT
                        a.id AS assignment_id,
                        a.prayer_id,
                        a.assigned_at,
                        a.completed_at,
                        p.id,
                        p.name,
                        p.request,
                        p.created_at,
                        p.visibility,
                        p.answered_at
                    FROM assignments a
                    JOIN prayers p
                        ON p.id =
                            a.prayer_id
                    WHERE a.participant_id =
                        $1
                        AND a.completed_at
                            IS NOT NULL
                    ORDER BY
                        a.completed_at DESC
                    LIMIT 100
                    `,
                    [participant.id]
                );

            const assignmentPrayerIds = [
                ...activeAssignments.rows,
                ...completedAssignments.rows
            ].map(
                row => row.prayer_id
            );

            const counts =
                await reactionCounts(
                    assignmentPrayerIds
                );

            const mapAssignment =
                row => ({
                    ...row,

                    prayer_count:
                        counts[
                        String(
                            row.prayer_id
                        )
                        ] || 0
                });

            /*
                PRIVATE ROOM-ONLY PRAYERS
            */

            let roomPrayers = [];

            if (participant.room_id) {
                const result =
                    await pool.query(
                        `
                        SELECT
                            rp.id,
                            rp.room_id,
                            rp.participant_id,
                            rp.request,
                            rp.is_temporary,
                            rp.created_at,
                            creator.name
                                AS creator_name
                        FROM room_prayers rp
                        LEFT JOIN participants creator
                            ON creator.id =
                                rp.participant_id
                        WHERE rp.room_id =
                            $1
                        ORDER BY
                            rp.created_at DESC
                        `,
                        [
                            participant.room_id
                        ]
                    );

                const roomIds =
                    result.rows.map(
                        row => row.id
                    );

                const roomCounts =
                    await roomPrayerReactionCounts(
                        roomIds
                    );

                roomPrayers =
                    result.rows.map(
                        row => ({
                            ...row,

                            prayer_count:
                                roomCounts[
                                String(
                                    row.id
                                )
                                ] || 0,

                            can_delete:
                                String(
                                    row.participant_id
                                ) ===
                                String(
                                    participant.id
                                ) ||
                                participant.owner_token ===
                                token
                        })
                    );
            }

            res.json({
                participant: {
                    id:
                        participant.id,

                    name:
                        participant.name
                },

                room: {
                    id:
                        participant.room_id,

                    code:
                        participant.room_code,

                    name:
                        participant.room_name ||
                        "Public Prayer Room",

                    is_private:
                        Boolean(
                            participant.is_private
                        ),

                    owner:
                        participant.owner_token ===
                        token
                },

                roomCount:
                    people.length,

                participants:
                    people.map(
                        person => ({
                            id:
                                person.id,

                            name:
                                person.name,

                            joined_at:
                                person.joined_at
                        })
                    ),

                activeAssignments:
                    activeAssignments.rows.map(
                        mapAssignment
                    ),

                completedAssignments:
                    completedAssignments.rows.map(
                        mapAssignment
                    ),

                /*
                    Compatibility with older room.html.
                */
                prayers:
                    activeAssignments.rows.map(
                        mapAssignment
                    ),

                roomPrayers
            });
        } catch (e) {
            dbError(res, e);
        }
    }
);


/* =========================================================
   COMPLETE / PRAY FOR ASSIGNED PUBLIC PRAYER
   ========================================================= */

app.post(
    "/api/room/assignments/:id/pray",
    async (req, res) => {
        const client =
            await pool.connect();

        try {
            const assignmentId =
                Number(
                    req.params.id
                );

            const token =
                String(
                    req.body.token || ""
                ).trim();

            if (
                !Number.isInteger(
                    assignmentId
                ) ||
                assignmentId <= 0 ||
                !isValidToken(token)
            ) {
                return res.status(400).json({
                    error:
                        "Valid assignment ID and room token are required."
                });
            }

            const participant =
                await getParticipantByToken(
                    token
                );

            if (!participant) {
                return res.status(404).json({
                    error:
                        "Participant not found."
                });
            }

            await client.query(
                "BEGIN"
            );

            const assignment =
                await client.query(
                    `
                    SELECT
                        a.id,
                        a.prayer_id,
                        a.completed_at
                    FROM assignments a
                    WHERE a.id = $1
                        AND a.participant_id = $2
                    FOR UPDATE
                    `,
                    [
                        assignmentId,
                        participant.id
                    ]
                );

            if (!assignment.rowCount) {
                await client.query(
                    "ROLLBACK"
                );

                return res.status(404).json({
                    error:
                        "Prayer assignment not found."
                });
            }

            const a =
                assignment.rows[0];

            if (!a.completed_at) {
                const reactorToken =
                    token +
                    "_room_prayer_" +
                    a.prayer_id;

                try {
                    await client.query(
                        `
                        INSERT INTO prayer_reactions(
                            prayer_id,
                            participant_id,
                            reactor_token
                        )
                        VALUES(
                            $1,
                            $2,
                            $3
                        )
                        `,
                        [
                            a.prayer_id,
                            participant.id,
                            reactorToken
                        ]
                    );
                } catch (e) {
                    if (
                        e.code !==
                        "23505"
                    ) {
                        throw e;
                    }
                }

                await client.query(
                    `
                    UPDATE assignments
                    SET completed_at =
                        NOW()
                    WHERE id = $1
                        AND participant_id =
                            $2
                        AND completed_at
                            IS NULL
                    `,
                    [
                        assignmentId,
                        participant.id
                    ]
                );
            }

            await client.query(
                "COMMIT"
            );

            const count =
                await pool.query(
                    `
                    SELECT
                        COUNT(*)::int
                            AS count
                    FROM prayer_reactions
                    WHERE prayer_id = $1
                    `,
                    [a.prayer_id]
                );

            await broadcast();

            res.json({
                success: true,
                count:
                    count.rows[0]
                        .count
            });
        } catch (e) {
            try {
                await client.query(
                    "ROLLBACK"
                );
            } catch { }

            dbError(res, e);
        } finally {
            client.release();
        }
    }
);

/* =========================================================
   PRIVATE ROOM-ONLY PRAYERS
   ========================================================= */

/*
    Add a prayer inside a private room.

    saved = true:
        stays permanently in room
        until deleted.

    saved = false:
        temporary. Removed when nobody
        remains active in the room.
*/

app.post(
    "/api/room/prayers",
    async (req, res) => {

        try {

            const token =
                String(
                    req.body.token || ""
                ).trim();


            const request =
                String(
                    req.body.request || ""
                ).trim();


            const type =
                req.body.type === "saved"
                    ? "saved"
                    : "temporary";


            if (!request) {

                return res.status(400).json({
                    error:
                        "Prayer request is required."
                });

            }


            const participant =
                await getParticipantByToken(
                    token
                );


            if (!participant) {

                return res.status(404).json({
                    error:
                        "Participant not found."
                });

            }


            if (!participant.room_id) {

                return res.status(400).json({
                    error:
                        "Room prayers are only available inside a private room."
                });

            }


            const result =
                await pool.query(
                    `
                    INSERT INTO room_prayers
                    (
                        room_id,
                        participant_id,
                        request,
                        is_temporary
                    )

                    VALUES
                    (
                        $1,
                        $2,
                        $3,
                        $4
                    )

                    RETURNING *
                    `,
                    [
                        participant.room_id,
                        participant.id,
                        request,
                        type === "temporary"
                    ]
                );


            await broadcast();


            return res.status(201).json({

                success: true,

                prayer:
                    result.rows[0],

                message:
                    type === "saved"
                        ? "Room prayer saved."
                        : "Temporary room prayer added."

            });

        }

        catch (error) {

            return dbError(
                res,
                error
            );

        }

    }
);

/*
    I PRAYED for a room-only prayer.
*/

app.post(
    "/api/room/prayers/:id/react",
    async (req, res) => {
        try {
            const roomPrayerId =
                Number(
                    req.params.id
                );

            const token =
                String(
                    req.body.token || ""
                ).trim();

            if (
                !Number.isInteger(
                    roomPrayerId
                ) ||
                roomPrayerId <= 0 ||
                !isValidToken(token)
            ) {
                return res.status(400).json({
                    error:
                        "Valid room prayer ID and room token are required."
                });
            }

            const participant =
                await getParticipantByToken(
                    token
                );

            if (!participant) {
                return res.status(404).json({
                    error:
                        "Participant not found."
                });
            }

            if (!participant.room_id) {
                return res.status(400).json({
                    error:
                        "This participant is not inside a private room."
                });
            }

            const prayer =
                await pool.query(
                    `
                    SELECT id
                    FROM room_prayers
                    WHERE id = $1
                        AND room_id = $2
                    `,
                    [
                        roomPrayerId,
                        participant.room_id
                    ]
                );

            if (!prayer.rowCount) {
                return res.status(404).json({
                    error:
                        "Private room prayer not found."
                });
            }

            let reacted = true;

            try {
                await pool.query(
                    `
                    INSERT INTO room_prayer_reactions(
                        room_prayer_id,
                        participant_id
                    )
                    VALUES(
                        $1,
                        $2
                    )
                    `,
                    [
                        roomPrayerId,
                        participant.id
                    ]
                );
            } catch (e) {
                if (
                    e.code ===
                    "23505"
                ) {
                    reacted = false;
                } else {
                    throw e;
                }
            }

            const count =
                await pool.query(
                    `
                    SELECT
                        COUNT(*)::int
                            AS count
                    FROM room_prayer_reactions
                    WHERE room_prayer_id =
                        $1
                    `,
                    [roomPrayerId]
                );

            if (reacted) {
                await broadcast();
            }

            res.json({
                success: true,
                reacted,
                count:
                    count.rows[0]
                        .count
            });
        } catch (e) {
            dbError(res, e);
        }
    }
);

/*
    DELETE private-room-only prayer.

    Allowed:

    - prayer creator
    - private room owner
*/

app.delete(
    "/api/room/prayers/:id",
    async (req, res) => {

        try {

            const prayerId =
                Number(
                    req.params.id
                );


            const token =
                String(
                    req.body.token || ""
                ).trim();


            const participant =
                await getParticipantByToken(
                    token
                );


            if (!participant) {

                return res.status(404).json({
                    error:
                        "Participant not found."
                });

            }


            if (!participant.room_id) {

                return res.status(400).json({
                    error:
                        "You are not inside a private room."
                });

            }


            const result =
                await pool.query(
                    `
                    DELETE FROM room_prayers rp

                    WHERE rp.id = $1

                      AND rp.room_id = $2

                      AND (
                          rp.participant_id = $3

                          OR EXISTS (
                              SELECT 1

                              FROM rooms r

                              WHERE r.id =
                                    rp.room_id

                                AND r.owner_token =
                                    $4
                          )
                      )

                    RETURNING rp.id
                    `,
                    [
                        prayerId,
                        participant.room_id,
                        participant.id,
                        token
                    ]
                );


            if (!result.rows.length) {

                return res.status(403).json({
                    error:
                        "You do not have permission to delete this room prayer."
                });

            }


            await broadcast();


            return res.json({
                success: true,
                message:
                    "Room prayer deleted."
            });

        }

        catch (error) {

            return dbError(
                res,
                error
            );

        }

    }
);
/* =========================================================
   ROUTES / SOCKET.IO
   ========================================================= */

app.get(
    "/admin",
    (req, res) => {
        res.sendFile(
            path.join(
                __dirname,
                "public",
                "admin.html"
            )
        );
    }
);

io.on(
    "connection",
    async () => {
        try {
            await emitRoom();
        } catch (e) {
            console.error(
                "socket connection:",
                e.message
            );
        }
    }
);

/* =========================================================
   DATABASE STARTUP / MIGRATION
   ========================================================= */

async function start() {
    try {
        console.log(
            "Checking database connection..."
        );

        await pool.query(
            "SELECT NOW()"
        );

        console.log(
            "Database connection OK."
        );

        /*
            Existing prayers compatibility.
        */

        await pool.query(`
            ALTER TABLE prayers
            ADD COLUMN IF NOT EXISTS
                answered_at
                TIMESTAMPTZ NULL
        `);

        /*
            Existing participants compatibility.
        */

        await pool.query(`
            ALTER TABLE participants
            ADD COLUMN IF NOT EXISTS
                room_id
                UUID NULL
        `);

        /*
            Private rooms.
        */

        await pool.query(`
            CREATE TABLE IF NOT EXISTS rooms (
                id UUID PRIMARY KEY,

                code TEXT NOT NULL UNIQUE,

                name TEXT NOT NULL
                    DEFAULT 'Private Prayer Room',

                is_private BOOLEAN NOT NULL
                    DEFAULT TRUE,

                owner_token TEXT NOT NULL,

                created_at TIMESTAMPTZ NOT NULL
                    DEFAULT NOW()
            )
        `);

        /*
            Connect participants to rooms.
        */

        await pool.query(`
            DO $$
            BEGIN

                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname =
                        'participants_room_id_fkey'
                ) THEN

                    ALTER TABLE participants
                    ADD CONSTRAINT
                        participants_room_id_fkey
                    FOREIGN KEY (
                        room_id
                    )
                    REFERENCES rooms(id)
                    ON DELETE SET NULL;

                END IF;

            END
            $$;
        `);

        /*
            Room-only prayers.
        */

        await pool.query(`
            CREATE TABLE IF NOT EXISTS room_prayers (
                id BIGINT
                    GENERATED BY DEFAULT
                    AS IDENTITY
                    PRIMARY KEY,

                room_id UUID NOT NULL
                    REFERENCES rooms(id)
                    ON DELETE CASCADE,

                participant_id UUID NULL
                    REFERENCES participants(id)
                    ON DELETE SET NULL,

                request TEXT NOT NULL,

                is_temporary BOOLEAN NOT NULL
                    DEFAULT TRUE,

                created_at TIMESTAMPTZ NOT NULL
                    DEFAULT NOW()
            )
        `);

        /*
            Room-only prayer reactions.
        */

        await pool.query(`
            CREATE TABLE IF NOT EXISTS
                room_prayer_reactions (

                id BIGINT
                    GENERATED BY DEFAULT
                    AS IDENTITY
                    PRIMARY KEY,

                room_prayer_id BIGINT NOT NULL
                    REFERENCES room_prayers(id)
                    ON DELETE CASCADE,

                participant_id UUID NOT NULL
                    REFERENCES participants(id)
                    ON DELETE CASCADE,

                created_at TIMESTAMPTZ NOT NULL
                    DEFAULT NOW(),

                UNIQUE(
                    room_prayer_id,
                    participant_id
                )
            )
        `);

        /*
            IMPORTANT ASSIGNMENT FIX

            The old assignments table made prayer_id
            globally UNIQUE.

            That prevents a prayer from being assigned
            again after someone finishes praying for it.

            We remove that old constraint.

            Then we create a partial unique index so that:

                ONE active assignment per prayer

            is allowed, while completed historical
            assignments can remain.
        */

        await pool.query(`
            ALTER TABLE assignments
            DROP CONSTRAINT IF EXISTS
                assignments_prayer_id_key
        `);

        await pool.query(`
            CREATE UNIQUE INDEX IF NOT EXISTS
                uq_active_assignment_per_prayer
            ON assignments(prayer_id)
            WHERE completed_at IS NULL
        `);

        /*
            Helpful indexes.
        */

        await pool.query(`
            CREATE INDEX IF NOT EXISTS
                idx_rooms_code
            ON rooms(code)
        `);

        await pool.query(`
            CREATE INDEX IF NOT EXISTS
                idx_participants_room_id
            ON participants(room_id)
        `);

        await pool.query(`
            CREATE INDEX IF NOT EXISTS
                idx_room_prayers_room_id
            ON room_prayers(room_id)
        `);

        await pool.query(`
            CREATE INDEX IF NOT EXISTS
                idx_room_prayers_created_at
            ON room_prayers(created_at DESC)
        `);

        await pool.query(`
            CREATE INDEX IF NOT EXISTS
                idx_room_prayer_reactions_prayer
            ON room_prayer_reactions(
                room_prayer_id
            )
        `);

        console.log(
            "Database tables ready."
        );

        await maintenance();

        server.listen(
            PORT,
            () => {
                console.log(
                    `Prayer Room running at http://localhost:${PORT}`
                );
            }
        );

    } catch (e) {
        console.error(
            "Unable to start server:",
            e.message
        );

        process.exit(1);
    }
}

start();