# Stage 2 Transformation Test Results

This document records the verification performed for GitHub Issue #8. It
covers the implemented and approved Stage 2 behavior from Issues #4–#7.

## Scope

Verified:

- Core Parquet transformation from Issue #4.
- Validation and quarantine from Issue #5.
- Transformation logging and metadata from Issue #6.
- Approved B-01 dataset integration from Issue #7.
- Record reconciliation, raw-input preservation, output schemas, and physical
  Parquet types.

Excluded:

- B-02, B-03, and B-04.
- Database loading or a load stage.
- Automatic pipeline orchestration.

Cleaning rules C-01–C-05 and standardization rules S-01–S-05 remain proposed
and unapproved in `docs/transformation/data_transformation.md`. They were not
implemented or tested as transformations. Their acceptance remains pending
approval of exact mappings, target types, and failure handling.

## Test commands

```text
python -m unittest discover -s tests -p 'test_stage2_verification.py' -v
python -m unittest discover -s tests -p 'test*.py' -v
```

Focused verification result: **4 tests passed**.

Complete suite result: **69 tests passed**.

Both commands completed successfully with status `OK`.

## Verified behavior

The verification tests check that:

- B-01 produces the documented integrated and quarantine schemas.
- Integrated and quarantined transactions reconcile to the input count.
- Unmatched transactions retain their source fields and exclusion reason.
- Raw input Parquet files remain unchanged.
- Repeated B-01 execution is deterministic.
- Serialized Parquet physical types match the documented contract.
- Validation quarantine records include batch and rule metadata.
- Validation metadata can be serialized by the transformation logger.
- No business rules are reported when no rules are supplied.

## Known limitations

- Cleaning and standardization cannot be verified until their proposed rules
  receive approval and complete specifications.
- B-01 publication rolls back ordinary exceptions but is not crash-safe across
  process termination or machine interruption.
- This verification does not add orchestration or database loading.
