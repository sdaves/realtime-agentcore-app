# Spec: RAG pipeline modernization

Status: ready-for-agent

## Problem Statement

The `real-time-rag-agentcore` app works, but it cannot be changed safely. Every behaviour lives inside a deployable script that begins executing the moment it is imported: the streaming job resolves job arguments, builds AWS clients, and starts Spark at module scope; the vector search function reads credentials and constructs clients at import time. Nothing can be called from a test, so there are no tests, and there is no linting or dependency manifest either.

The consequences are already visible. The README describes a conditional-upsert mechanism that the code does not implement. The streaming job builds its embedding inputs and its documents as two parallel lists whose indices must stay aligned, so a single failed record can attach one event's embedding to another event's text. "Freshest event per entity" is decided by comparing timestamp strings, which is only correct if every producer happens to use the same textual format. The CDK stack's deploy notes reference a setup script that is not in the repository.

A developer who wants to fix any of this has no way to know whether they have broken the pipeline short of deploying to AWS, pushing events, and waiting twenty seconds per query.

## Solution

Restructure the app so that its logic can be exercised as ordinary Python. The rules of the pipeline — which records are usable, what text gets embedded, which duplicate wins, how the search endpoint validates and responds — become plain functions over plain data, callable from tests with no AWS account, no credentials, no Spark, and no network. Deployable scripts keep only the wiring that genuinely needs to run on the platform.

Along the way, the defects the restructure exposes are fixed: embeddings are bound to their documents structurally rather than by list position, and freshness is decided by instant rather than by string. The library-level dependency versions become single-sourced so the streaming job and the search function cannot drift apart, and the documentation is brought back in line with what actually deploys.

## User Stories

1. As a maintainer, I want to run the app's checks with one command, so that I can tell whether a change is safe before touching AWS.
2. As a maintainer, I want the checks to pass without credentials or network access, so that they run on any machine and in CI.
3. As a maintainer, I want a dependency manifest that pins the versions the stack installs, so that a local run and a deployed run cannot resolve different libraries.
4. As a maintainer, I want the streaming job's per-record rules in a function I can call, so that I can reason about them without a Spark session.
5. As a maintainer, I want to assert what text an event becomes, so that a change to the embedded text is a visible test diff rather than a silent retrieval regression.
6. As a maintainer, I want a record with no usable identifier to be dropped explicitly, so that junk never reaches the vector store under an empty key.
7. As a maintainer, I want entity identifiers normalised in exactly one place, so that the search side and the indexing side cannot disagree about the form.
8. As a maintainer, I want metadata to parse whether it arrives as a mapping or as a JSON string, so that schema inference quirks on the stream do not produce unreadable documents.
9. As a maintainer, I want an event's embedding to be attached to that event's document, so that a single bad record cannot cause the pipeline to serve one order's status under another order's identifier.
10. As a maintainer, I want a failing record isolated to that record, so that one malformed event does not drop the rest of its micro-batch.
11. As a maintainer, I want exactly one document per entity per batch, so that the store does not accumulate near-duplicate rows for a hot entity.
12. As a maintainer, I want the surviving document to be the freshest by instant, so that an event that arrives late but is newer still wins.
13. As a maintainer, I want freshness to be independent of timestamp formatting, so that a producer switching from `Z` to an explicit offset does not silently invert the ordering.
14. As a maintainer, I want the search function to import cleanly without credentials, so that its request handling can be tested in isolation.
15. As a maintainer, I want the search function's AWS dependencies injected, so that a test can substitute them without patching module globals.
16. As a maintainer, I want a missing query rejected with a clear client error, so that a misconfigured agent invocation does not look like a pipeline outage.
17. As a maintainer, I want an absurd result count clamped to the documented maximum, so that a caller cannot ask the store for an unbounded result set.
18. As a maintainer, I want the response shape and timings to be asserted, so that the agent-side contract cannot drift unnoticed.
19. As a maintainer, I want failures to surface as a server error with a clean body, so that internal detail is not handed to the caller.
20. As a maintainer, I want the infrastructure split into one construct per concern, so that I can find the resource I care about without reading three hundred lines.
21. As a maintainer, I want the synthesised template asserted, so that a refactor of the stack cannot silently drop a resource, a grant, or an output.
22. As a maintainer, I want the cross-construct wiring asserted, so that the job can still read its script and the search function can still reach the collection.
23. As a maintainer, I want the deploy and teardown documented against the real resources, so that following the README does not leave a collection billing by the month.
24. As a maintainer, I want no documentation pointing at files that do not exist, so that the next reader is not sent chasing a missing script.
25. As a maintainer, I want the version stated in the docs to be the version actually used, so that local setup instructions work on the first attempt.
26. As a maintainer, I want the design notes to describe the mechanism the code implements, so that the rationale I read matches the behaviour I debug.

## Implementation Decisions

- **Two function-level test seams, not one.** The streaming job and the search function are packaged as separate artifacts (a script asset uploaded to S3 for Glue; a bundled directory for Lambda). A single shared library would need its code injected into both artifacts at build time. We accept two seams over introducing shared packaging, because the packaging change carries more risk than the seam count saves. A third seam — the synthesised CloudFormation template — covers the infrastructure.
- **Seam one: pipeline rules over plain data.** The transformation from a raw event to the document that gets indexed, and the rule that selects the freshest document per entity, are pure functions taking and returning plain Python values. They hold no AWS clients, no Spark context, and no module-level side effects.
- **Seam two: the search handler with injected collaborators.** The handler receives its embedding and search dependencies rather than constructing them at import. Production wiring supplies the real clients; tests supply substitutes. The Lambda entry point remains a thin adapter over this seam.
- **Seam three: template assertions.** The stack is split into constructs and verified by synthesising the app and asserting on the resulting template, so wiring is checked without deploying.
- **Embeddings are bound to documents, not to list positions.** The structure that pairs an embedding with its document makes misalignment unrepresentable, rather than relying on two lists staying the same length and order.
- **Freshness compares instants.** Timestamps are parsed before comparison; the textual formats the producers emit (`Z`-suffixed and explicit-offset forms of the same instant) order correctly against each other.
- **Dependency versions are single-sourced.** The library versions the streaming job receives via its additional Python modules and the versions the search function is bundled with must agree, and both must agree with the local development manifest.
- **The published search contract is unchanged.** Request fields, response fields, status codes, and the maximum result count stay as documented; only the ability to test them is new.
- **Deployable entry points stay thin.** The streaming job script and the Lambda handler keep the responsibilities that require platform context — Spark session, Glue job lifecycle, client construction — and delegate the rules to the seam.
- **The freshness mechanism discrepancy is resolved in favour of the code.** The implemented approach indexes without explicit document identifiers and selects freshness on read. The spec treats the README's conditional-upsert claim as documentation drift to be corrected, not as a missing feature to be built, unless the maintainer says otherwise.

## Testing Decisions

- A good test here calls a seam with plain input and asserts on plain output. It does not assert on the number of client calls, does not inspect private helpers, and does not restate the implementation. If the assertion would still pass with the logic inverted, it is not a test.
- Seam one is tested with dictionaries standing in for stream records: well-formed order updates, metadata arriving as both a mapping and a JSON string, records missing an identifier or a timestamp, and duplicate entities whose freshest member is not the last one in arrival order.
- Seam two is tested by invoking the handler with substitutes for embedding generation and search: a missing query, an over-large result count, a multi-hit success, and an injected failure in each dependency. The module must import with no credentials present, which is itself asserted.
- Seam three is tested by synthesising the stack and asserting that the expected resources, grants, and outputs exist, and that the streaming job's script asset and the search function's environment reference the resources they should.
- No prior art exists in this app; these are its first tests. The repo's general convention — assert on external behaviour through the highest available seam, one logical assertion per test, no mocking of internal collaborators — applies.
- Tests must not require Spark, AWS credentials, network access, or Docker.
- Per the app's own bug class, one test is written to fail before its fix: the misalignment case in seam one and the mixed-format ordering case in freshness. Each is demonstrated red, then green.

## Out of Scope

- Deploying anything, or running any agent or CDK command against a real account.
- The Strands agent's prompt, model choice, or its Gateway tool discovery — its runtime behaviour needs a live Gateway and is not reachable from these seams.
- Mocking Spark or Glue. The streaming job's Spark wiring is verified by synthesis and by the extracted rules, not by simulating a Spark session.
- Adding AgentCore Memory, AgentCore Policy, or any new pipeline capability.
- The macOS metadata file dropped by the upstream project, and any other vendored-file hygiene beyond what the ticket criteria state.
- Introducing pre-commit hooks or CI. If wanted, the repo's own pre-commit guidance targets a JavaScript workspace and would need adapting first.
- Re-deriving the app's original provenance. The app was vendored from an upstream repository that carries no licence, and was relicensed under MIT at the maintainer's direction; that decision is recorded, not revisited here.

## Further Notes

- The defects listed in the Problem Statement were found by reading the vendored code, not by running it. The misalignment and ordering issues are latent — they require a malformed record or a mixed-format producer to surface. The documentation discrepancies and the missing setup script are unambiguous.
- The README's stated demo cost and its teardown checklist are the highest-consequence documentation in the app, since leaving an OpenSearch Serverless collection running is the expensive failure mode. Ticket 06 treats that checklist as load-bearing.
- Nothing in this spec changes the published API surface, so the app remains usable by anyone following the existing README until ticket 06 lands.
