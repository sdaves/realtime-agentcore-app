# llms

Agent skills harness plus deployable reference apps.

## Apps

| App | Description |
| --- | --- |
| [`real-time-rag-agentcore`](apps/real-time-rag-agentcore) | Real-time RAG on AWS: Kinesis → Glue Streaming → OpenSearch Serverless → AgentCore Gateway + Runtime (Strands Agents). |

Each app directory is self-contained: its own `README.md`, dependencies, and (where applicable) `LICENSE`.

## Agent conventions

- **Issue tracker** — issues live as markdown under `.scratch/<feature>/`. See [`docs/agents/issue-tracker.md`](docs/agents/issue-tracker.md).
- **Triage labels** — the five canonical roles. See [`docs/agents/triage-labels.md`](docs/agents/triage-labels.md).
- **Domain docs** — one `GLOSSARY.md` and `docs/adr/` at the repo root. See [`docs/agents/domain.md`](docs/agents/domain.md).
