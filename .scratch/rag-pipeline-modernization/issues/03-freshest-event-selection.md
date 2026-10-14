# 03: Freshest-event selection for duplicate entities

**What to build:** When the same entity appears more than once in a micro-batch, exactly one document survives — the one with the latest timestamp — decided by instant rather than by string. Two representations of the same moment, one `Z`-suffixed and one with an explicit offset, order correctly against each other, so a producer changing its formatting convention cannot silently invert which event wins.

**Blocked by:** 02.

**Status:** ready-for-agent

- [ ] Within a batch, exactly one document per entity identifier is retained.
- [ ] The retained document is the freshest by instant, including when the freshest member is not the last in arrival order.
- [ ] Ordering is consistent across every timestamp format the producers emit, not just the one currently used.
- [ ] A record whose timestamp cannot be interpreted never displaces a record whose timestamp can.
- [ ] Tests cover duplicates arriving out of order, and the same instant written in a `Z`-suffixed and an explicit-offset form.
- [ ] A test demonstrating the mixed-format ordering failure fails before the fix and passes after it.
