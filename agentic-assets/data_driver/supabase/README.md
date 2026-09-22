# Supabase data source

Rows of **one** Supabase table, read over PostgREST (`GET /rest/v1/<table>`). Each row becomes a
record; the row's primary key is the record's identity, so editing a row in Supabase updates the
record in place instead of minting a second one.

Watching two tables means two sources.

## The credential — Flowpad holds it

The API key is **never** a field on the form. The manifest declares it:

```json
"auth": {"secrets": {"api_key": "ingest_api.supabase"}}
```

Flowpad resolves that, in this order:

1. the secret store this source row is bound to, under the key `api_key`;
2. otherwise the machine secret **`ingest_api.supabase`** — Flowpad's own secret store.

**Your step:** store the key as the machine secret `ingest_api.supabase` (Flowpad → Settings →
Secrets → add `ingest_api.supabase`). Use the project's **service-role** key to read a table
outright; the **anon** key works too but reads only what row-level security lets an anonymous
reader see — an empty sync with a healthy source is usually that.

Find both in Supabase → Project Settings → **Data API** (the Project URL is on the same page).

## Configuration

| Field | Required | What it does |
| --- | --- | --- |
| `project_url` | yes | `https://<ref>.supabase.co`. Names which project this row serves. A self-hosted PostgREST gateway works. |
| `table` | yes | The one table this source reads. The picker lists what the key can see. |
| `db_schema` | no (`public`) | Only a schema exposed under Data API settings is readable. |
| `id_column` | no (`id`) | The column that identifies a row. **Get this wrong and edits mint duplicates.** |
| `order_column` | no (key column) | What "newest" means, and where the record's date comes from — usually `created_at`. |
| `title_column` | no | Which column names the record. Empty: the first of `title`, `name`, `subject`, `full_name`, `company_name`, `email` the row has. |
| `columns` | no (`*`) | A PostgREST select list: `id,company_name,status`, or an embed `id,owner(name)`. |
| `filters` | no | PostgREST predicates, one per line: `status=eq.new`, `score=gte.80`. All must hold. |
| `max_rows` | no (`200`) | Rows per pass. |

## What a pass does

A pass reads the **newest `max_rows` rows by `order_column`**, every time, and the ingestor's
digest absorbs the ones that have not changed. It is a ceiling, not a queue: a table that grows by
more than `max_rows` between passes leaves its tail unread — raise `max_rows`, or narrow the source
with `filters`.

No cursor is carried between passes. PostgREST has no change feed, and a key-ordered offset would
step over a row inserted behind it.

## Setting one up

```bash
flow source create supabase \
  --name "Leads" \
  -c project_url=https://<ref>.supabase.co \
  -c table=leads \
  -c order_column=created_at \
  -c title_column=company_name \
  -c filters=status=eq.new

flow source verify <id>   # names the one thing to fix: the key, the table, or the schema
flow source sync   <id>   # health ok + what it wrote
flow source items  <id>   # the records
```

## When it does not work

`verify` answers with the one thing to change:

- **"Supabase refused the stored key"** — the secret is missing, wrong, or it is an anon key and
  RLS hides the table. Store the service-role key as `ingest_api.supabase`.
- **"Supabase has no `<schema>.<table>` on its Data API"** — check the spelling, and that the
  schema is exposed under Project Settings → Data API.
- A sync that is healthy but **empty** is almost always RLS: the key can reach the table and sees
  no rows through it.
