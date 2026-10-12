"""The check registry is the supported extension point."""

import os

from review import REGISTERED_CHECKS, STEPS
from review.checks import CheckSpec, run_checks
from review.config import DEFAULTS
from review.context import Finding, ReviewContext
from review.report import AREAS

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SKILL_DIR = os.path.join(ROOT, "workspace-review")


def test_registry_metadata_is_valid_and_unique():
    ids = [check.check_id for check in REGISTERED_CHECKS]
    assert len(ids) == len(set(ids))
    assert all(ids)

    areas = {area for area, _ in AREAS}
    stages = [name for name, _ in STEPS]
    assert stages.index("jobs") < stages.index("compute")
    for check in REGISTERED_CHECKS:
        assert check.area in areas
        assert check.after in stages
        assert set(check.config_keys) <= set(DEFAULTS)
        assert all(path.count(".") == 1 for path in check.required_stats)
        assert all(stage in stages and stages.index(stage) < stages.index(check.after)
                   for stage in check.depends_on)


def test_every_registered_check_is_documented():
    with open(os.path.join(SKILL_DIR, "references", "checks.md")) as handle:
        docs = handle.read()
    assert {check.check_id for check in REGISTERED_CHECKS} <= set(docs.replace(":", " ").split())


def test_one_failed_check_does_not_stop_the_next_check():
    ctx = ReviewContext.create(spark=None, skill_dir=SKILL_DIR)

    def fail(_ctx):
        raise RuntimeError("broken check")

    def pass_(target):
        target.add_finding(Finding("TEST-PASS", "uc", "LOW", "title", "summary", "impact", "recommendation"))

    checks = (
        CheckSpec("TEST-FAIL", "uc", "test", fail),
        CheckSpec("TEST-PASS", "uc", "test", pass_),
    )
    run_checks(ctx, checks, "test")

    assert [finding.check_id for finding in ctx.findings] == ["TEST-PASS"]
    assert ctx.errors == [{"object": "TEST-FAIL", "step": "check", "error": "broken check"}]
