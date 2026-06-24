# Quality Gates

## Purpose

Quality gates define the checks that must pass before code or data can move forward in the pipeline.

## Code Quality Gates

The GitHub Actions CI workflow validates:

1. Python syntax
2. Ruff linting
3. Secret scanning
4. Unit tests

If any check fails, the pipeline stops.

## Data Quality Gates

The PySpark pipeline validates:

1. Bronze row count
2. Silver row count
3. Gold row count
4. Required columns are not null
5. Duplicate records are not present
6. Numeric values are within accepted ranges

## Metadata Driven Checks

Validation rules are defined in:

```text
config/metadata/source_metadata.yaml