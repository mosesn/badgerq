package com.mosesn.finagle_batching

import com.twitter.finagle.Service
import com.twitter.util.{Await, Duration, Future, Promise, Time, Timer}

abstract class BatchingService[Req, Rep](
  factory: ServiceFactory[Seq[Req], Seq[Rep]]
) extends Service[Req, Rep] {
  @volatile var stopped = false

  def apply(req: Req): Future[Rep] = {
    if (stopped) {
      return Future.exception(new Exception("failed"))
    }
    produce(req)
  }

  def produce(req: Req): Future[Rep]

  override def close(deadline: Time): Future[Unit] = {
    stopped = true
    Future.Done
  }

  def fulfilBatch(svc: Service[Seq[Req], Seq[Rep]], pairs: Seq[(Req, Promise[Rep])]): Future[Unit] = {
    (svc(pairs map (_._1)) onSuccess { results =>
      if (results.size == pairs.size) {
        results.zip(pairs).map({ case (result, (_, p)) =>
          p.setValue(result)
        })
      } else {
        for ((_, p) <- pairs) yield {
          p.setException(new Exception("failed"))
        }
      }
    } onFailure { exc =>
      for ((_, p) <- pairs) {
        p.setException(exc)
      }
    }).unit
  }
}
