"""Explicit registry and runner for workspace review checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Tuple

from .context import ReviewContext


@dataclass(frozen=True)
class CheckSpec:
    """Metadata needed to validate, schedule, and run one check."""

    check_id: str
    area: str
    after: str
    evaluate: Callable[[ReviewContext], None]
    config_keys: Tuple[str, ...] = ()
    required_stats: Tuple[str, ...] = ()
    depends_on: Tuple[str, ...] = ()


REGISTERED_CHECKS = []


def register_checks(checks: Iterable[CheckSpec]) -> None:
    REGISTERED_CHECKS.extend(checks)


def registered_areas():
    return {check.area for check in REGISTERED_CHECKS}


def _has_stat(ctx: ReviewContext, path: str) -> bool:
    area, _, name = path.partition(".")
    return bool(name) and name in ctx.stats.get(area, {})


def run_checks(ctx: ReviewContext, checks: Iterable[CheckSpec], after: str) -> None:
    """Run matching checks independently so one failure cannot hide the rest."""
    for check in checks:
        if check.after != after:
            continue
        missing = [path for path in check.required_stats if not _has_stat(ctx, path)]
        if missing:
            ctx.note(f"Check `{check.check_id}` skipped; required data was not collected: {', '.join(missing)}.")
            continue
        try:
            check.evaluate(ctx)
        except Exception as exc:
            ctx.record_error(check.check_id, "check", exc)
            ctx.note(f"Check `{check.check_id}` failed: {str(exc).splitlines()[0][:200]}")
