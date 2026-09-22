"""``SupabaseSource`` — rows of ONE Supabase table, read over PostgREST.

Supabase's REST surface IS PostgREST: ``GET /rest/v1/<table>``, the project's API key in both
``apikey`` and ``Authorization``, the schema chosen by ``Accept-Profile``. One source reads one
table, because a table is the unit a person thinks about and the unit a dataset is later defined
over; someone watching leads and deals adds two sources.

Identity is the row's primary key, so editing a row in Supabase UPDATES its record instead of
minting a second one. That is the whole reason ``id_column`` is a form field rather than a guess:
a wrong key column is not a slow sync, it is duplicates.

A pass is a WINDOW, not a queue — the newest ``max_rows`` by ``order_column``, every time, with
the ingestor's digest absorbing the rows that have not changed. A table that grows by more than
``max_rows`` between passes leaves its tail unread: a ceiling, stated here, rather than a backlog
nothing reports. No cursor is carried between passes (``durable_cursor`` is False) because
PostgREST has no change feed, and a key-ordered offset would step over a row inserted behind it.

The key is never config. It is declared in the manifest's ``auth`` and Flowpad resolves it — the
machine secret ``ingest_api.supabase``, or ``api_key`` in the store this source is bound to.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Annotated, Any, ClassVar, Mapping, Optional, Sequence
from urllib.parse import quote, urlsplit

from pydantic import Field, StringConstraints

from flow_sdk.sources import http
from flow_sdk.sources.base import CollectionSource
from flow_sdk.sources.binding import SourceBinding
from flow_sdk.sources.config import SourceConfig
from flow_sdk.sources.errors import AccessDenied, NotFound, Rejected, SourceError
from flow_sdk.sources.protocols import Verdict
from flow_sdk.sources.values.items import FeedItemData, SourceItemSpec
from flow_sdk.sources.values.origin import CloudOrigin

#: Where the application keeps the key: a MACHINE secret, because a Supabase project is account
#: bound and an ingest source has no project of its own to hold a project-scoped credential.
SECRET_NAME = "ingest_api.supabase"
#: PostgREST's ceiling for one round-trip. Never a retry budget — there is no retry.
REQUEST_TIMEOUT_SECONDS = 20
#: Rows one pass reads when the config names no other number.
DEFAULT_MAX_ROWS = 200
DEFAULT_SCHEMA = "public"
DEFAULT_ID_COLUMN = "id"
#: Columns that name a row when nobody said which one does, in the order a person would try.
TITLE_COLUMNS = ("title", "name", "subject", "full_name", "company_name", "email")

_NO_KEY = (
    f"No Supabase API key. Flowpad holds it, not this form: store the project's service-role key "
    f"(or its anon key, for tables a reader may see) as the machine secret '{SECRET_NAME}', or as "
    f"'api_key' in the secret store this source is bound to."
)

#: A SQL identifier. These reach PostgREST as query parameters, so the pattern is what keeps a
#: column name from smuggling a second parameter in beside itself.
_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"

Ident = Annotated[str, StringConstraints(strip_whitespace=True, pattern=rf"^{_IDENT}$")]
OptionalIdent = Annotated[str, StringConstraints(strip_whitespace=True, pattern=rf"^(?:{_IDENT})?$")]


class SupabaseRowData(FeedItemData):
    """One row. Nothing is volatile: a column that changed IS the change worth seeing — which is
    the opposite of a feed's vote count, and the reason this class says so out loud."""

    spec_kind: ClassVar[str] = "ingest.feed.item.supabase"

    table: Optional[str] = None
    row: Optional[dict] = None


class SupabaseConfig(SourceConfig):
    """What a supabase source is configured with."""

    project_url: Annotated[str, StringConstraints(strip_whitespace=True, pattern=r"^https?://[^\s/?#]+")]
    table: Ident
    db_schema: Ident = DEFAULT_SCHEMA
    id_column: Ident = DEFAULT_ID_COLUMN
    order_column: OptionalIdent = ""
    title_column: OptionalIdent = ""
    #: A PostgREST select list: ``id,company_name`` or an embed, ``id,owner(name)``.
    columns: Annotated[str, StringConstraints(strip_whitespace=True, pattern=r"^[A-Za-z0-9_,.:()*\- ]+$")] = "*"
    #: ``status=eq.new`` — one predicate per entry. No ``&``: one filter is one parameter.
    filters: list[Annotated[str, StringConstraints(strip_whitespace=True, pattern=rf"^{_IDENT}=[^&#]+$")]] = []
    max_rows: Annotated[int, Field(ge=1, le=1000)] = DEFAULT_MAX_ROWS


class SupabaseSource(CollectionSource):

    Config = SupabaseConfig
    provider = "supabase"
    identity_config_key = "project_url"
    durable_cursor = False

    def __init__(self, binding: SourceBinding) -> None:
        super().__init__(binding)
        self._client: Any = None
        self._entries: Optional[list[tuple[str, dict]]] = None

    # ── the config, read the way this class means it ────────────────────────
    @property
    def rest_url(self) -> str:
        base = str(self.config.get("project_url") or "").strip().rstrip("/")
        if not base:
            raise Rejected("This Supabase source needs its project URL (config.project_url).")
        return f"{base}/rest/v1"

    @property
    def table(self) -> str:
        table = str(self.config.get("table") or "").strip()
        if not table:
            raise Rejected("This Supabase source needs its table (config.table).")
        return table

    @property
    def db_schema(self) -> str:
        return str(self.config.get("db_schema") or "").strip() or DEFAULT_SCHEMA

    @property
    def id_column(self) -> str:
        return str(self.config.get("id_column") or "").strip() or DEFAULT_ID_COLUMN

    @property
    def order_column(self) -> str:
        """What "newest" means here — the key column when the table carries no timestamp."""
        return str(self.config.get("order_column") or "").strip() or self.id_column

    def origin(self, key: str, *within: str) -> CloudOrigin:
        """Primary keys repeat across tables, so the table is part of the scope."""
        return super().origin(key, *(within or (self.table,)))

    async def _open(self) -> None:
        self._client, self._entries = http.client(REQUEST_TIMEOUT_SECONDS), None

    async def _close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ── read ────────────────────────────────────────────────────────────────
    def query(self) -> None:
        """The table IS the scope: ``filters`` and ``columns`` shape it, and there is nothing left
        for a query to parameterize."""
        return None

    async def _scan(self, query: Any) -> Sequence[tuple[str, dict]]:
        """The pass's window, once per session — the paging contract slices this list, and a list
        that changed under it mid-traversal would drop rows between pages."""
        if self._entries is None:
            rows = await self._rows(limit=int(self.config.get("max_rows") or DEFAULT_MAX_ROWS))
            keyed = [(_key_of(row, self.id_column), row) for row in rows]
            # One row without the key column costs that row, not the pass: it has no identity to
            # update in place, so recording it would mint a duplicate on every single sync.
            self._entries = sorted(((key, row) for key, row in keyed if key), key=lambda entry: entry[0])
        return self._entries

    async def _lookup(self, key: str) -> Optional[dict]:
        """Straight at the row, not within the window: ``get`` is asked about rows the window has
        long since scrolled past."""
        rows = await self._rows(limit=1, where={self.id_column: f"eq.{key}"})
        return rows[0] if rows else None

    def _item(self, key: str, raw: dict) -> SourceItemSpec:
        link = self._dashboard_url()
        data = SupabaseRowData(
            title=self._title(key, raw),
            text=_as_text(raw),
            url=link,
            published_at=_when(raw.get(self.order_column)),
            table=self.table,
            row=raw,
        )
        origin = self.origin(key)
        return SourceItemSpec(origin=origin.model_copy(update={"url": link}) if link else origin, data=data)

    def _title(self, key: str, raw: dict) -> Optional[str]:
        named = str(self.config.get("title_column") or "").strip()
        if named:
            return _label(raw.get(named))
        for candidate in TITLE_COLUMNS:
            if (label := _label(raw.get(candidate))) is not None:
                return label
        return f"{self.table} {key}"

    def _dashboard_url(self) -> Optional[str]:
        """Supabase's own table editor for this project. A formula over the project URL, because a
        row has no public address — and ``None`` for a self-hosted gateway, which has no such page."""
        host = urlsplit(str(self.config.get("project_url") or "")).hostname or ""
        ref = host.split(".")[0] if host.endswith(".supabase.co") else ""
        return f"https://supabase.com/dashboard/project/{ref}/editor" if ref else None

    # ── setup ───────────────────────────────────────────────────────────────
    async def verify(self) -> Verdict:
        """Can the key Flowpad holds read this table? Everything else here is config.

        Each refusal names the ONE thing a person changes, because "it does not work" and "expose
        the schema in Data API settings" are not the same sentence.
        """
        if not str(self.config.get("project_url") or "").strip():
            return Verdict(ready=False, detail="Set the Supabase project URL this source reads.", pending=("project_url",))
        if not self.credentials.values.get("api_key"):
            return Verdict(ready=False, detail=_NO_KEY, pending=("api_key",))
        try:
            async with self:
                await self._rows(limit=1)
        except AccessDenied:
            return Verdict(
                ready=False,
                pending=("api_key",),
                detail=f"Supabase refused the stored key for {self.db_schema}.{self.table}. An anon key reads only "
                f"what row-level security lets it; the service-role key reads the table outright.",
            )
        except NotFound:
            return Verdict(
                ready=False,
                pending=("table",),
                detail=f"Supabase has no {self.db_schema}.{self.table} on its Data API. Check the spelling, and that "
                f"the schema is exposed under Project Settings → Data API.",
            )
        except SourceError as exc:
            return Verdict(ready=False, detail=f"Supabase did not answer for {self.db_schema}.{self.table}: {exc}")
        return Verdict(ready=True, detail=f"Reading {self.db_schema}.{self.table}.")

    async def choices(self, field: str) -> list[dict]:
        """The tables this key can see, from PostgREST's own OpenAPI document — so nobody has to
        remember a name the database already knows. Read for THIS call only."""
        if field != "table":
            return []
        if not self.credentials.values.get("api_key"):
            raise AccessDenied(_NO_KEY)
        body = await self._api("GET", "/")
        return [{"id": name, "name": name} for name in _table_names(body)]

    # ── transport ───────────────────────────────────────────────────────────
    async def _rows(self, *, limit: int, where: Optional[Mapping[str, str]] = None) -> list[dict]:
        params: dict[str, Any] = {
            "select": str(self.config.get("columns") or "*").strip() or "*",
            "order": f"{self.order_column}.desc",
            "limit": limit,
        }
        for entry in self.config.get("filters") or ():
            name, _, predicate = str(entry).partition("=")
            if name and predicate:
                params[name] = predicate
        params.update(where or {})
        body = await self._api("GET", f"/{quote(self.table, safe='')}", params=params)
        if not isinstance(body, list):
            raise Rejected(f"Supabase answered GET /{self.table} with {type(body).__name__}, not a list of rows")
        return [row for row in body if isinstance(row, dict)]

    async def _api(self, verb: str, path: str, **kwargs: Any) -> Any:
        secret = self.credentials.values.get("api_key")
        if secret is None or not secret.get_secret_value():
            raise AccessDenied(_NO_KEY)
        token = secret.get_secret_value()
        headers = {
            "apikey": token,
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            # PostgREST selects the schema per request; a non-public one is otherwise unreachable.
            "Accept-Profile": self.db_schema,
        }
        url = f"{self.rest_url}{path}"
        hint = f"{verb} {path}"
        if self._client is not None:
            response = await http.request(self._client, verb, url, headers=headers, hint=hint, **kwargs)
        else:  # verify and choices run on a source nobody opened
            async with http.client(REQUEST_TIMEOUT_SECONDS) as client:
                response = await http.request(client, verb, url, headers=headers, hint=hint, **kwargs)
        try:
            return response.json()
        except ValueError as exc:
            raise Rejected(f"Supabase answered {hint} with undecodable JSON") from exc


def _key_of(row: dict, id_column: str) -> str:
    value = row.get(id_column)
    return str(value).strip() if isinstance(value, (str, int, float)) else ""


def _label(value: Any) -> Optional[str]:
    return text if isinstance(value, (str, int, float)) and (text := str(value).strip()) else None


def _as_text(row: dict) -> Optional[str]:
    """The row as lines a person and the digest can both read; the structured copy rides in ``row``."""
    lines = [f"{name}: {_flat(value)}" for name, value in row.items() if value not in (None, "", [], {})]
    return "\n".join(lines) or None


def _flat(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)


def _table_names(body: Any) -> list[str]:
    """Table names out of PostgREST's OpenAPI document — ``definitions`` on the Swagger 2.0 shape
    it has always served, ``paths`` on anything that stops serving them."""
    if not isinstance(body, dict):
        return []
    names = list((body.get("definitions") or {}).keys())
    if not names:
        names = [path.lstrip("/") for path in (body.get("paths") or {}) if path not in ("", "/")]
    return sorted({name for name in names if re.fullmatch(_IDENT, name or "")})


def _when(value: Any) -> Optional[datetime]:
    """A Postgres timestamp as an aware datetime. A naive one is UTC: ``timestamp`` without a zone
    is stored as UTC by every Supabase default, and guessing local time would shift every record."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace(" ", "T", 1).replace("Z", "+00:00")
    if re.search(r"[+-]\d{2}$", text):  # Postgres writes `+00`; fromisoformat wants `+00:00`
        text += ":00"
    try:
        stamp = datetime.fromisoformat(text)
    except ValueError:
        return None
    return stamp if stamp.tzinfo is not None else stamp.replace(tzinfo=timezone.utc)


__all__ = [
    "DEFAULT_MAX_ROWS",
    "DEFAULT_SCHEMA",
    "REQUEST_TIMEOUT_SECONDS",
    "SECRET_NAME",
    "TITLE_COLUMNS",
    "SupabaseConfig",
    "SupabaseRowData",
    "SupabaseSource",
]
