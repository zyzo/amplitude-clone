# Agent loop

## Run server

- Backend: `npm run dev`
- Frontend: `npm run dashboard:dev`

## Develop

Implement the requested changes, using the backend and frontend servers as needed.

## Verify

### Frontend

- Run `npm run dashboard:build` and `npm run dashboard:test` when frontend code changes.

### Backend

- Run `npm run build` and relevant tests with `npm test` when backend code changes. Use real PostgreSQL for database behavior; smoke-test Docker when container or startup behavior changes.

Fix failures and repeat verification until checks pass. For documentation-only changes, check accuracy and skip runtime tests.

## Report

Report what changed, what passed, any unplanned impact, and any blockers.
