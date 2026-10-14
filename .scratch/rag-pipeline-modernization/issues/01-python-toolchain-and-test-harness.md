# 01: Python toolchain and fast test harness

**What to build:** The app becomes a single reproducible Python 3.12 project. One manifest declares the runtime and development dependencies at the versions the stack already installs, and lint plus test runners are wired so that a clean checkout passes with no AWS credentials, no network access, and no Spark session. These two commands become the app's definition of green, and every later ticket is verified with them.

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [x] A Python 3.12 project manifest declares the app's dependencies, and the versions agree with the ones the stack installs for the streaming job and the search function — no unpinned `>=` drift between manifest, Glue, and Lambda.
- [x] Development tooling is declared separately from deploy dependencies, so neither the Glue script asset nor the Lambda bundle ships test tooling.
- [x] `ruff check` and a format check run clean across the app.
- [x] `pytest` runs from the app directory and exits 0 with no credentials, no network, and no Spark install.
- [x] At least one test exercises a real seam in the app today rather than asserting on the harness itself. The event generator's pure event-construction rule is the natural first target.
- [x] The Python version stated in the app's documentation matches the manifest and the devcontainer.

## Comments

Completed in `45768ab`, with the mechanical sweep isolated in `5eeb269`.

**Evidence.** Verified against a fresh clone at HEAD with a fresh virtualenv, not the working tree: `ruff check` clean, `ruff format --check` reports all eight files formatted, `pytest` 8 passed in 0.10s. No credentials, network calls, or Spark were involved.

**Single-sourcing.** `pyproject.toml` is the manifest; `requirements.txt` was deleted rather than left alongside it, since keeping both is the drift this ticket exists to remove. `cdk_stack.py` reads `[project].dependencies` at synth time and interpolates it into Glue's `--additional-python-modules` and the Lambda bundling command. Verified by executing those code paths out of the real source: the Glue argument resolves to `boto3==1.43.114,opensearch-py==3.2.0,requests-aws4auth==1.4.0` and the bundling line to `pip install opensearch-py==3.2.0 requests-aws4auth==1.4.0 -t /asset-output`.

**Two deviations worth recording.**

1. `opensearch-py` pinned to `3.2.0`, a major bump from the upstream `>=2.4.0`. The imports the app actually uses (`OpenSearch`, `RequestsHttpConnection`, `AWS4Auth`) all resolve under 3.2.0, verified by import rather than assumed.
2. `boto3` is requested for Glue only. The Lambda runtime already provides it, so bundling it would be redundant. This is an intentional asymmetry, not drift.

**Caveats.**

- Full `cdk synth` could not be run: it needs Node for the jsii bridge and Docker for the Lambda asset bundling, and neither is installed in this environment. The dependency-loading logic was therefore verified by executing its real AST nodes, not the whole stack. Ticket 05 will need Node and Docker available to assert on synthesised templates.
- Glue 4.0 runs Python 3.10, so the streaming job is outside the manifest's `==3.12.*` range by construction. The manifest governs the repo, the local toolchain, and the search function; the streaming job's runtime is Glue's business. Ticket 02 should keep the extracted rules 3.10-compatible for that reason.
- `ruff check --fix` plus `ruff format` reformatted five vendored modules, ending the byte-identical copy of the upstream source. The logic-bearing reformat is in the build commit and the mechanical-only reformat is isolated in the style commit. No behaviour changed: the diff is quote style, f-string prefixes, `timezone.utc` → `UTC` (the same object), import order, and re-wrapping.
- Two lint rules are deliberately not satisfied by code: `E501` is ignored because it fires on prose inside the agent's system prompt and on long operator-facing literals, and `F403` is ignored for the streaming job's `awsglue` wildcard import, which only resolves on the Glue runtime. Ticket 02 should narrow that wildcard import to explicit names when it extracts the job.
- The dead `event_id` read in the micro-batch was removed to satisfy the toolchain. The event id is consequently still not part of an indexed document; ticket 02 should decide whether it belongs there.
