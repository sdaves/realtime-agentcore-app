# 04: Vector search contract, testable without AWS

**What to build:** The search function's request handling — validating the query, capping the requested result count, shaping the JSON response, and turning a failure into a server error instead of a crash — is exercisable in a test with the embedding and search collaborators substituted. Importing the module no longer performs AWS calls or requires credentials to be present.

**Blocked by:** 01.

**Status:** ready-for-agent

- [ ] The module imports successfully with no AWS credentials and no network access.
- [ ] The embedding and search dependencies are supplied to the handler rather than constructed at import time, and production wiring still supplies the real ones.
- [ ] A missing or empty query returns a client error with a JSON error body, without invoking the embedding model.
- [ ] The requested result count is clamped to the documented maximum, and a nonsensical value is rejected or clamped rather than passed through to the store.
- [ ] A successful search returns a success response with the documented shape, including the result count and both timings.
- [ ] A failure in embedding or search returns a server error with a JSON error body, without leaking credentials or internal detail to the caller.
- [ ] The Lambda entry point remains a thin adapter over the tested seam.
- [ ] Tests cover a missing query, an over-large result count, a search returning several hits, and an injected failure in each collaborator.
