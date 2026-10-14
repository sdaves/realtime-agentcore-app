# 05: Split the CDK stack into constructs

**What to build:** The single long stack file becomes a small app entry point plus one construct per concern — ingress, vector store, streaming job, search function, and monitoring — so a reader can find the resource they care about without scrolling past everything else. The synthesised template keeps the same resources, grants, and outputs, and assertions prove the wiring that is easiest to break silently: the job's script reaching its bucket, the search function reaching its collection, and both principals holding their data-access grants.

**Blocked by:** 01.

**Status:** ready-for-agent

- [ ] The stack synthesises to the same resources and deployment outputs as before the split, verified by assertions rather than by inspection.
- [ ] Each construct owns one concern and declares the inputs it needs explicitly, rather than reaching into the parent stack for them.
- [ ] The streaming job's script asset is still deployed from the job's script directory, and the search function's environment still references the real collection endpoint and index.
- [ ] Both the streaming job's role and the search function's role retain the grants they need against the stream, the bucket, the model, and the collection.
- [ ] The data-access policy still names both principals.
- [ ] Deployment outputs — stream name, collection endpoint, function ARN and name, alarm topic, dashboard URL — are preserved.
- [ ] No resource is declared twice and no orphaned policy survives the split.
- [ ] Template assertions run as part of the app's test suite, requiring no deployment.
