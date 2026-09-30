# Agent loop

1. Build the smallest complete change.
2. Verify with `npm run build` and relevant tests via `npm test`. Use real PostgreSQL for database behavior; smoke-test Docker when container or startup behavior changes.
3. Fix failures and repeat until checks pass.
4. Report what changed, what passed, unplanned impact, and any blockers.

For documentation-only changes, check accuracy; skip runtime tests.
