# Prayer Room — Supabase PostgreSQL + Socket.IO

This version uses the Supabase PostgreSQL database directly through `pg` and Socket.IO for realtime browser updates.

## Features

- Public and private prayer requests
- Public prayer wall
- Private prayers visible to Admin
- Anyone can explicitly delete a prayer
- Delete works for public and private prayers
- Deleting a prayer removes its reactions and Prayer Room assignment
- Assigning a prayer to the Prayer Room does NOT remove it from the prayer list
- Realtime Prayer Room participant count
- Realtime room presence updates
- Realtime prayer assignment updates
- Realtime delete updates
- Prayer Room automatically distributes public prayers among active participants
- “I Prayed for This” reaction/count
- Admin filters: all/public/private, date, search, newest/oldest/most prayed
- Admin participant list

## Important behavior

A Prayer Room assignment is only a reference to the original prayer.

Submitting/assigning a public prayer:

`prayers` -> remains in public wall + admin + assignment

Marking it prayed:

`assignments.completed_at` -> updated, prayer remains

Explicitly deleting it:

`prayer_reactions` -> removed by CASCADE
`assignments` -> removed by CASCADE
`prayers` -> removed

## Setup

1. Create the database tables by opening the Supabase SQL Editor and running `schema.sql`.
2. Copy `.env.example` to `.env`.
3. Put your Supabase PostgreSQL database password in `DB_PASSWORD`.
4. Run:

```bash
npm install
npm start
```

5. Open:

```text
http://localhost:3000
```

Admin:

```text
http://localhost:3000/admin
```

Use the `ADMIN_USERNAME` and `ADMIN_PASSWORD` from `.env`.

## Realtime room behavior

The browser sends a heartbeat every 3 seconds while a person is in the room. The server considers a participant active for 10 seconds after their last heartbeat. The server checks presence every 2 seconds and broadcasts changes through Socket.IO.

Therefore, when the last person leaves or their browser stops sending heartbeats, connected pages update the room count to `0` automatically after the timeout.

## Security note

This project intentionally allows anyone to delete a prayer because that was requested. That means the DELETE endpoint is not admin-protected. Before using this publicly, consider adding authentication, ownership checks, moderation, or a confirmation/authorization mechanism.

Never put `DB_PASSWORD` in frontend JavaScript or commit `.env` to GitHub.
