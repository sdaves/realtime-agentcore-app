# 06: Reconcile the documentation with deployed reality

**What to build:** The documentation stops describing a pipeline that does not exist. The claims that disagree with the code — the conditional-upsert design note, where freshest-per-entity selection actually happens, the Python version — are corrected to match what deploys, and the deploy notes stop pointing at a script that is not in the repository. The teardown checklist is treated as load-bearing, because its failure mode is a collection that bills by the month.

**Blocked by:** 02, 03, 04, 05.

**Status:** ready-for-agent

- [ ] The design-decisions section describes the mechanism the code actually uses to keep the freshest event per entity, and no longer claims a conditional-upsert implementation that is absent.
- [ ] No README, deploy note, or stack comment points at a file, script, or command that is absent from the app.
- [ ] The stated Python version is the one the toolchain and the search function's runtime actually use.
- [ ] The documented deploy, verify, and teardown steps are checked against the resources the split stack really creates, and the teardown still covers every resource that incurs ongoing cost.
- [ ] The file tree in the README matches the app's real layout after extraction and the stack split.
- [ ] The documented example invocations still match the published request and response contract.
