# 02: Extract the event-to-document transform

**What to build:** The streaming job's per-record rules — deciding whether a record is usable, normalising the entity identifier, parsing metadata, and composing the text that gets embedded — become a pure function over plain dictionaries that can be called and asserted with no Spark and no AWS. The pairing of an embedding with its document becomes structural, so a single failing record can no longer attach one event's embedding to another event's text.

**Blocked by:** 01.

**Status:** ready-for-agent

- [ ] Given a batch of raw event dictionaries, the transform returns the documents to be indexed: embedded text, entity identifier, event type, timestamp, metadata, and an unfilled embedding slot.
- [ ] A record missing a usable entity identifier or timestamp is dropped explicitly, rather than indexed under an empty key.
- [ ] Entity identifier normalisation for the vector collection's document-id rules happens in exactly one place and is observable from a test.
- [ ] Metadata arriving as a JSON string and as a mapping both end up as a mapping, and malformed metadata does not raise.
- [ ] An embedding is only ever attached to the document it was generated from: a record that fails partway through cannot shift any other record's embedding.
- [ ] Tests cover a well-formed order update, metadata as a string, a record with no timestamp, and a record with malformed metadata.
- [ ] A test demonstrating the misalignment case fails against the current parallel-list behaviour before the fix and passes after it.
