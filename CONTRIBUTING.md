# Contributing Guide

## Branch Strategy

This repository uses three branches with different roles:

- main: stable production branch without bundled test datasets and test-only tooling.
- test: development and validation branch for experiments, performance checks, and test assets.
- master: frozen legacy branch. Do not use for active development.

## Required Workflow

1. Start all implementation work in the `test` branch.
2. Validate behavior and performance in `test`.
3. If changes are confirmed as improvements, open a Pull Request from `test` to `main`.
4. Merge only reviewed and validated changes into `main`.

## Rules

- Do not develop directly in `main`.
- Do not develop directly in `master`.
- Keep `main` clean: no bundled test datasets, no test-only scripts/tools.
- Keep test datasets and test helpers in `test` branch.

## Branch Protection (GitHub)

Recommended repository settings:

- Protect `main`: require Pull Request and review before merge.
- Protect `test`: allow controlled direct pushes for active testing.
- Protect `master`: restrict pushes and merges (frozen branch).

## Emergency Fixes

For urgent production fixes:

1. Create a short-lived branch from `main`.
2. Implement and verify the fix.
3. Open PR to `main`.
4. Sync the fix back to `test`.

## Commit and PR Recommendations

- Use clear commit messages describing intent.
- Keep PR scope focused.
- Include short test notes in PR description:
  - what was checked,
  - expected impact,
  - rollback notes if applicable.
