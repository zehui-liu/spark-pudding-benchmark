# Bug report template

English | [简体中文](bug-report-template.md)

Copy this template to `results/runs/<case-id>/bug.md` or an external issue tracker.

```markdown
# [component] short symptom

## Environment

- spark-pudding branch/commit:
- Distribution path:
- JDK:
- Spark:
- Docker image tag:
- Case ID:
- Data profile: smoke / scale (row count)

## Configuration

- Job file: `config/jobs/<case-id>.yaml`
- Env file: `config/env/<name>.properties`
- Connection/catalog used:
- Secret references used:

## Reproduction steps

1.
2.
3.

Minimal reproduction command:

```shell
```

## Expected result

## Actual result

## Evidence

- Result directory: `results/runs/<case-id>/`
- Key logs:
- Target-side query result:
- Checkpoint/state status:

## Impact

- ⬜ Data loss
- ⬜ Duplicate data
- ⬜ Wrong result
- ⬜ Silent success
- ⬜ Recovery failure
- ⬜ Credential leak
- ⬜ Performance/resource
- ⬜ Documentation/example only

## Severity

P0 / P1 / P2 / P3

## Initial area

core / source / sink / transform / runtime / client / docs

## Post-fix regression

- Smoke case:
- Scale case:
- New assertions:
```

## Severity definitions

| Level | Definition | Example |
| --- | --- | --- |
| P0 | Data loss, duplication, wrong result, silent success, credential leak | Upsert overwrites with the wrong value; failure returns exit code 0 |
| P1 | Feature unavailable, recovery failure, clear error but cannot continue | CDC cannot start; checkpoint cannot recover |
| P2 | Edge behavior, error message, documentation/example issue | Example misses a required field; misleading message |
| P3 | Usability, performance, non-blocking warning | Excessive logs; missing metric |
