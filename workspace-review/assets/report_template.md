<!-- Filled in by scripts/review/report.py. Placeholders are names in double braces; every one must be filled. -->
# Workspace review: {{workspace_name}}

_Generated {{generated_at}} · areas: {{area_scope}} · catalogs: {{catalog_scope}} · schemas: {{schema_scope}} · activity window {{activity_window_days}} days · run by {{current_user}}_

> **Drill-down:** every affected object for each finding, with its fix command, is in the notebook
> `{{notebook_path}}`. This report shows the top items only.

## Executive summary

| Area | Score | Status | 🔴 High | 🟠 Medium | 🟡 Low |
|---|---|---|---|---|---|
{{score_rows}}

Scores start at 100 per area; each finding subtracts {{score_weights}}.

### Top findings
{{top_findings}}

### Quick wins
{{quick_wins}}

## Workspace overview
{{overview}}

## Findings by area
{{findings_by_area}}

## Hive → Unity Catalog migration health
{{migration_section}}

## Workspace settings
{{settings_section}}

---

## Technical appendix

### Detailed findings and evidence
{{evidence}}

### Remediation commands
Commands are also saved to `{{remediation_file}}`. Review each one before running it: most rewrite whole tables.

{{remediation_commands}}

### Coverage and data sources
{{coverage}}

### Settings used
{{settings}}
