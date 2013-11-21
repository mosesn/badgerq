## TODO
testing
  - hammer it a little to try to find a concurrency issue
  - test conjunctive
  - test disjunctive
  - test the Running semantics
documentation
  - more fleshed out README
  - document the semantics of QueueingDiscipline and BatchingService
name
  - still not a good name
timed semantics
  - should not start until after first production
  - currently supports > duration, should support < duration
asyncsemaphore
  - X concurrent requests
conjunctive discipline
  - bundling can be smarter
