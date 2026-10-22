"""Shared state, findings model, and read-only SQL helpers."""

from __future__ import annotations

import copy
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from .config import DEFAULTS, REVIEW_AREAS

SEVERITIES = ("HIGH", "MEDIUM", "LOW")
SEVERITY_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
SEVERITY_ICON = {"HIGH": "🔴", "MEDIUM": "🟠", "LOW": "🟡"}
GB = 1024 ** 3

# The review never changes anything in the workspace. Every statement goes through run_sql,
# which only allows statements that start with one of these keywords.
_READ_ONLY_PREFIXES = ("SELECT", "WITH", "DESCRIBE", "DESC", "SHOW")


class NotReadOnlyError(ValueError):
    pass


def assert_read_only(statement: str) -> None:
    stripped = _strip_sql_comments(statement).strip().upper()
    if not stripped.startswith(_READ_ONLY_PREFIXES):
        raise NotReadOnlyError(f"Refusing to run a non read-only statement: {statement[:80]!r}")
    if ";" in stripped.rstrip(";"):
        raise NotReadOnlyError("Refusing to run multiple statements in one call")


def _strip_sql_comments(statement: str) -> str:
    statement = re.sub(r"/\*.*?\*/", " ", statement, flags=re.S)
    return re.sub(r"--[^\n]*", " ", statement)


def parse_schema_spec(raw: str) -> tuple:
    """Split an include_schemas entry into (catalog_or_None, schema), lowercased."""
    spec = str(raw).strip()
    parts = spec.split(".")
    if len(parts) == 1 and parts[0]:
        return None, parts[0].lower()
    if len(parts) == 2 and parts[0] and parts[1]:
        return parts[0].lower(), parts[1].lower()
    raise ValueError(f"include_schemas entry must be a schema name or catalog.schema, not {raw!r}")


def quote_ident(*parts: str) -> str:
    """`cat`.`schema`.`table`, escaping backticks inside names."""
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


def sql_literal(value: Any) -> str:
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def load_named_queries(path: str) -> Dict[str, str]:
    """Parse a .sql file made of blocks that each start with `-- name: <query_name>`."""
    with open(path) as f:
        text = f.read()
    queries: Dict[str, str] = {}
    current = None
    lines: List[str] = []
    for line in text.splitlines():
        m = re.match(r"\s*--\s*name:\s*(\w+)\s*$", line)
        if m:
            if current:
                queries[current] = "\n".join(lines).strip().rstrip(";")
            current, lines = m.group(1), []
        elif current:
            lines.append(line)
    if current:
        queries[current] = "\n".join(lines).strip().rstrip(";")
    return queries


def decimals_to_float(df):
    """Spark DECIMAL columns (e.g. billing usage_quantity) arrive as decimal.Decimal objects; make them floats."""
    for col in df.columns:
        if df[col].dtype == object and df[col].map(lambda v: isinstance(v, Decimal)).any():
            df[col] = df[col].map(lambda v: float(v) if isinstance(v, Decimal) else v)
    return df


def evidence_records(df) -> List[Dict[str, Any]]:
    """Convert a pandas DataFrame into JSON/CSV-friendly evidence records."""
    return df.astype(object).where(df.notna(), None).to_dict("records")


def plural(count: int, noun: str, plural_form: Optional[str] = None) -> str:
    """'1 table', '82 tables', with thousands separators."""
    return f"{count:,} {noun if count == 1 else (plural_form or noun + 's')}"


def principal_kind(name: Optional[str]) -> str:
    """Best-effort guess at what kind of principal owns an object."""
    if not name:
        return "none"
    if "@" in name:
        return "user"
    if re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", name.lower()):
        return "service_principal"
    return "group"


@dataclass
class Finding:
    check_id: str
    area: str
    severity: str
    title: str
    summary: str
    impact: str
    recommendation: str
    evidence: List[Dict[str, Any]] = field(default_factory=list)
    doc_url: Optional[str] = None
    # Copy-paste remediation statements; also written to remediation_<date>.sql.
    remediation_sql: List[str] = field(default_factory=list)

    def __post_init__(self):
        if self.severity not in SEVERITIES:
            raise ValueError(f"Unknown severity {self.severity}")


class SdkApi:
    """GET-only REST access through the Databricks SDK, using the notebook's own credentials.

    Raw REST calls (rather than typed SDK methods) so the review works with whatever SDK version the
    compute ships with.
    """

    def __init__(self):
        from databricks.sdk import WorkspaceClient
        self._client = WorkspaceClient().api_client

    def get(self, path: str, query: Dict[str, Any]) -> Dict[str, Any]:
        return self._client.do("GET", path, query=query)


@dataclass
class ReviewContext:
    spark: Any
    skill_dir: str
    config: Dict[str, Any]
    started_at: datetime
    current_user: Optional[str] = None
    workspace_id: Optional[int] = None
    workspace_url: Optional[str] = None
    # name -> True/False: is this data source readable by the current user?
    sources: Dict[str, bool] = field(default_factory=dict)
    # Free-text notes on gaps in coverage (shown in the report appendix).
    coverage_notes: List[str] = field(default_factory=list)
    # area -> stat name -> value (scalars, dicts, or pandas DataFrames).
    stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    findings: List[Finding] = field(default_factory=list)
    # Errors on individual objects (e.g. DESCRIBE DETAIL failed on one table).
    errors: List[Dict[str, str]] = field(default_factory=list)
    # REST access: any object with get(path, query) -> dict. Created on first use; tests inject a fake.
    api: Any = None

    @classmethod
    def create(cls, spark, skill_dir: str, activity_window_days: Optional[int] = None,
               include_catalogs: Optional[List[str]] = None,
               include_schemas: Optional[List[str]] = None,
               include_areas: Optional[List[str]] = None,
               overrides: Optional[Dict[str, Any]] = None) -> "ReviewContext":
        config = copy.deepcopy(DEFAULTS)
        unknown = set(overrides or {}) - set(DEFAULTS)
        if unknown:
            raise ValueError(f"Unknown setting(s): {sorted(unknown)}")
        config.update(overrides or {})
        if activity_window_days is not None:
            config["activity_window_days"] = int(activity_window_days)
        if include_catalogs is not None:
            config["include_catalogs"] = list(include_catalogs)
        if include_schemas is not None:
            config["include_schemas"] = list(include_schemas)
        if include_areas is not None:
            config["include_areas"] = list(include_areas)
        catalog = str(config["system_catalog"]).strip()
        if not catalog:
            raise ValueError("system_catalog must be a non-empty catalog name")
        config["system_catalog"] = catalog
        config["include_schemas"] = [str(s).strip() for s in config["include_schemas"] if str(s).strip()]
        for spec in config["include_schemas"]:
            parse_schema_spec(spec)
        aliases = {"tables": "uc"}
        config["include_areas"] = [
            aliases.get(a, a) for a in (
                str(raw).strip().lower() for raw in config["include_areas"]
            ) if a
        ]
        unknown_areas = [a for a in config["include_areas"] if a not in REVIEW_AREAS]
        if unknown_areas:
            raise ValueError(
                f"Unknown include_areas {unknown_areas}; choose from {list(REVIEW_AREAS)}")
        return cls(spark=spark, skill_dir=skill_dir, config=config,
                   started_at=datetime.now(timezone.utc))

    # --- REST (GET only) ---------------------------------------------------------
    def api_get(self, path: str, query: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """GET a workspace REST endpoint. There is deliberately no way to send any other method."""
        if self.api is None:
            self.api = SdkApi()
        return self.api.get(path, query or {}) or {}

    def api_list(self, path: str, key: str, query: Optional[Dict[str, Any]] = None,
                 max_pages: int = 200) -> List[Dict[str, Any]]:
        """GET every page of a list endpoint that uses next_page_token."""
        query, items = dict(query or {}), []
        for _ in range(max_pages):
            page = self.api_get(path, query)
            items.extend(page.get(key) or [])
            token = page.get("next_page_token")
            if not token:
                return items
            query["page_token"] = token
        self.note(f"Stopped listing {path} after {max_pages} pages; results are incomplete.")
        return items

    # --- SQL -------------------------------------------------------------------
    def run_sql(self, statement: str):
        """Run a read-only statement and return a Spark DataFrame."""
        assert_read_only(statement)
        return self.spark.sql(statement)

    def query_pdf(self, statement: str):
        """Run a read-only statement and return a pandas DataFrame (DECIMAL columns as floats)."""
        return decimals_to_float(self.run_sql(statement).toPandas())

    def queries(self, filename: str) -> Dict[str, str]:
        return load_named_queries(os.path.join(self.skill_dir, "scripts", "sql", filename))

    def render(self, sql: str, catalog_col: Optional[str] = None, schema_col: Optional[str] = None,
               **params) -> str:
        """Fill the standard placeholders used by the .sql files.

        `{catalog_filter}` needs `catalog_col`: the column (or expression) holding the catalog name.
        `{schema_filter}` is TRUE unless `include_schemas` is set; then it needs `schema_col`, and
        qualified catalog.schema entries also need `catalog_col`.
        """
        values = {
            "activity_window_days": int(self.config["activity_window_days"]),
            "workspace_id": int(self.workspace_id) if self.workspace_id is not None else "NULL",
            "system_catalog": quote_ident(self.system_catalog()),
            "schema_filter": "TRUE",
        }
        if catalog_col:
            values["catalog_filter"] = self.catalog_filter_sql(catalog_col)
        if schema_col:
            values["schema_filter"] = self.schema_filter_sql(schema_col, catalog_col)
        values.update(params)
        return sql.format(**values)

    def system_catalog(self) -> str:
        return str(self.config["system_catalog"]).strip()

    def system_table(self, *parts: str) -> str:
        """Unquoted `catalog.schema[.table]` for logs and coverage notes."""
        return ".".join((self.system_catalog(),) + parts)

    def excluded_catalogs(self) -> List[str]:
        names = list(self.config["excluded_catalogs"]) + list(self.config["extra_excluded_catalogs"])
        names.append(self.system_catalog())
        seen, unique = set(), []
        for name in names:
            key = name.lower()
            if key not in seen:
                seen.add(key)
                unique.append(key)
        return unique

    def included_areas(self) -> List[str]:
        """Areas this run covers. Empty include_areas means every implemented area."""
        return list(self.config["include_areas"] or REVIEW_AREAS)

    def area_in_scope(self, area: str) -> bool:
        return area in self.included_areas()

    def included_catalogs(self) -> List[str]:
        return [n.lower() for n in self.config["include_catalogs"]]

    def included_schema_specs(self) -> List[tuple]:
        """(catalog_or_None, schema) pairs, lowercased. Empty means all schemas."""
        return [parse_schema_spec(spec) for spec in self.config["include_schemas"]]

    def catalog_filter_sql(self, catalog_col: str) -> str:
        """SQL predicate keeping only in-scope catalogs: the include list (if any) minus the exclusions."""
        clauses = [f"lower({catalog_col}) NOT IN ({', '.join(sql_literal(n) for n in self.excluded_catalogs())})"]
        if self.included_catalogs():
            clauses.append(f"lower({catalog_col}) IN ({', '.join(sql_literal(n) for n in self.included_catalogs())})")
        return " AND ".join(clauses)

    def schema_filter_sql(self, schema_col: str, catalog_col: Optional[str] = None) -> str:
        """SQL predicate keeping only include_schemas (TRUE if that list is empty)."""
        specs = self.included_schema_specs()
        if not specs:
            return "TRUE"
        unqualified = [schema for catalog, schema in specs if catalog is None]
        qualified = [(catalog, schema) for catalog, schema in specs if catalog is not None]
        clauses = []
        if unqualified:
            clauses.append(f"lower({schema_col}) IN ({', '.join(sql_literal(n) for n in unqualified)})")
        if qualified:
            if not catalog_col:
                raise ValueError("Qualified include_schemas entries need catalog_col")
            pairs = ", ".join(f"({sql_literal(c)}, {sql_literal(s)})" for c, s in qualified)
            clauses.append(f"(lower({catalog_col}), lower({schema_col})) IN ({pairs})")
        return "(" + " OR ".join(clauses) + ")" if len(clauses) > 1 else clauses[0]

    def catalog_in_scope(self, catalog: str) -> bool:
        name = catalog.lower()
        return name not in self.excluded_catalogs() and (not self.included_catalogs() or name in self.included_catalogs())

    def schema_in_scope(self, catalog: str, schema: str) -> bool:
        specs = self.included_schema_specs()
        if not specs:
            return True
        catalog_name, schema_name = catalog.lower(), schema.lower()
        return any(schema_name == schema_spec and (cat is None or cat == catalog_name)
                   for cat, schema_spec in specs)

    # --- Results -----------------------------------------------------------------
    def add_stat(self, area: str, name: str, value: Any) -> None:
        self.stats.setdefault(area, {})[name] = value

    def add_finding(self, finding: Finding) -> None:
        self.findings.append(finding)

    def note(self, message: str) -> None:
        if message not in self.coverage_notes:
            self.coverage_notes.append(message)

    def record_error(self, obj: str, step: str, exc: Exception) -> None:
        self.errors.append({"object": obj, "step": step, "error": str(exc).splitlines()[0][:300]})
