# Prayer Room — Answered Prayer Version

Node.js + Express + PostgreSQL/Supabase + Socket.IO.

## Features
- Public and private prayer requests.
- Public active prayers and a separate Answered Prayers section.
- “I Prayed” increments the prayer count but does NOT mark a prayer answered.
- Public users can mark a public prayer as answered; it remains stored.
- Private prayers are only visible to the admin.
- Only the admin can permanently delete prayers.
- Admin can filter/search/sort, mark/unmark answered, and delete.
- Prayer Room assignments never delete prayers.
- Prayer Room participant count is realtime and reaches 0 after stale sessions expire.

## Setup
1. `npm install`
2. Run `schema.sql` in Supabase SQL Editor. If you already have the old project, the important migration is `ALTER TABLE prayers ADD COLUMN IF NOT EXISTS answered_at TIMESTAMPTZ NULL;`.
3. Copy `.env.example` to `.env` and fill in your database password and admin password.
4. `npm start`
5. Open `http://localhost:3000/`

Admin: `http://localhost:3000/admin`

Never commit `.env` or your database password.
