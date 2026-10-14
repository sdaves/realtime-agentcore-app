# 01: Python toolchain and fast test harness

**What to build:** The app becomes a single reproducible Python 3.12 project. One manifest declares the runtime and development dependencies at the versions the stack already installs, and lint plus test runners are wired so that a clean checkout passes with no AWS credentials, no network access, and no Spark session. These two commands become the app's definition of green, and every later ticket is verified with them.

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] A Python 3.12 project manifest declares the app's dependencies, and the versions agree with the ones the stack installs for the streaming job and the search function — no unpinned `>=` drift between manifest, Glue, and Lambda.
- [ ] Development tooling is declared separately from deploy dependencies, so neither the Glue script asset nor the Lambda bundle ships test tooling.
- [ ] `ruff check` and a format check run clean across the app.
- [ ] `pytest` runs from the app directory and exits 0 with no credentials, no network, and no Spark install.
- [ ] At least one test exercises a real seam in the app today rather than asserting on the harness itself. The event generator's pure event-construction rule is the natural first target.
- [ ] The Python version stated in the app's documentation matches the manifest and the devcontainer.
