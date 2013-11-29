## badgerq

Finagle traditionally trades throughput for latency.
This utility lets you make the trade the other way.

## api

There are two pieces of the public API, QueueingDiscipline and BatchingService.
BatchingService is Service[Req, Rep], and takes a ServiceFactory[Seq[Req], Seq[Rep]]
and a QueueingDiscipline, and queues requests until the QueueingDiscipline signals
it's ready.  When it's ready, it immediate sends all of the requests on the queue.
  
Although you can make your own QueueingDiscipline if you like, badgerq also provides
some built in ones.

1.  `SizeBatching` will batch when at least a certain number are queued.
2.  `TimerBatching` will batch when a request has been on the queue for at least a certain amount of time.
3.  `RateLimiting` will batch when there are fewer than a certain number of simultaneous requests.

Note that these disciplines are each structured as a logical rule.  Because they're all structured
as rules, we can make combinators that combine them.  If you have two QueueingDisciplines, you can
`and`, `not`, or `or` them together in a natural way.

## example

Because we have `and`, `not`, and `or`, we have enough power to form any logical statement.  We can
construct `implies` with the tools we have already.

```scala
// a implies b
// NB: a implies b === a or (not b)
def implies(a: QueueingDiscipline, b: QueueingDiscipline): QueuingDiscipline = {
    a or (b.not)
}
```

If we wanted to batch items after they had been on the queue for at least 50 milliseconds and there were
at least 10 of them, we could easily make a QueueingDiscipline to do that too.

```scala
val discipline = new SizeBatching(10) and new TimerBatching(50.milliseconds, timer)
```