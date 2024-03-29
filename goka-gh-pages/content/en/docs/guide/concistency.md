---
title: Delivery Semantics
linkTitle: "Delivery Semantics"
weight: 10
description: >
  Delivery Semantics and Crash handling
---
---

## Marking vs committing message offsets

Sometimes there is confusion around *marking* and *comitting* messages. *Marking* is done by the processor callback, after a message has been processed and all subsequent operations - like `SetValue` or `Emit` - succeeded. The marked offset is stored and frequently *committed* back to the brokers. On graceful processor shutdown, the offsets are committed one last time. Both operations are necessary, otherwise messages are reconsumed after processor restart.

Goka takes care of *marking* offsets as consumed automatically. To manually mark offsets, one can use [

## What happens if my processor crashes?

Is the message lost? Is the message resent? What happens to any side-effects?

### Short answer 
like Kafka, goka provides **at-least-once**-semantics for processors, so **a message might be received multiple times on unclean processor shutdown**.

### Long answer
Goka processes messages in parallel across partitions, but sequentially within one partition. Between partitions there are no guarantees, so let's consider a single partition.

Assume there are two messages in a partition with offsets 42 and 43.
The processor-callback is called first with message 42. When the callback returns, message 43 will be processed. 
That means however, that the side-effects (emitting other messages, updating the table-value) of message 42 may still be uncommitted before the processor handles message 43. The commit of 42 is only performed when all its side-effects are committed. And 43 will not be committed if 42 hasn't been committed yet.

The result is, there is no exactly-once guarantee in Goka. For example, if processing offset 42 generates messages A and B, and emitting of B fails, then A and B will be emitted again when 42 is reprocessed. So you'll see A twice downstream. Goka will give you at-least-once. 
If you need exactly-once, maybe take should look into ksql or kstreams. However, usually one doesn't need exactly-once and it's sufficient to make the processing of events idempotent.

## Delaying message commit in the callback