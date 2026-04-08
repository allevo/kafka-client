---
name: comments
description: Automatically adds useful inline code comments to new or changed code. Triggers after code edits.
---

# Code Comments Skill

After writing or editing code, review the changes you just made and add inline code comments (`//`) where they would help a reader **who lacks project context** understand the code.

## What makes a comment worth adding

A comment is worth adding when a reader without context would struggle to understand **why** the code does what it does. Good comments:

- Explain the **why** behind a design choice, not the what
- Reference protocol behavior, spec requirements, or external constraints that justify the approach
- Preempt confusion — if a reader might think "is this a bug?" or "why not do X instead?", the comment answers that before they ask
- Clarify non-obvious invariants or assumptions baked into the code

## Example of a good comment

```rust
// The Kafka protocol only requires correlation IDs to be unique among
// in-flight requests. The server echoes back whatever the client sends.
// Sequential generation is a convention (used by both the Java client
// and librdkafka), not a protocol requirement.
// For this reason, the sending order doesn't guarantee correlation ordering,
// i.e. the task can be yielded between this line and the send below.
// This is fine.
let correlation_id = self.next_correlation_id.fetch_add(1, Ordering::Relaxed);
```

This comment works because it explains why `Relaxed` ordering is safe by referencing the Kafka protocol spec and addressing the natural "is this a race?" concern.

## What to avoid

- Never restate what the code does (e.g. `// increment counter` above `counter += 1`)
- Never add comments to self-explanatory code
- Never add doc comments (`///` or `//!`) — those are handled separately
- Never add TODO/FIXME comments unless the code is intentionally incomplete
- Keep comments concise — a few lines max, not paragraphs

## How to apply

1. Look at the code you just wrote or changed
2. For each non-trivial block, ask: "would a reader without context understand why this exists or why it's done this way?"
3. If no — add a comment that gives them the missing context
4. If yes — leave it alone
5. Place the comment directly above the relevant line(s)
