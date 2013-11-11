package com.mosesn.finagle_batching

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Await, Duration, Future, Promise, Time, Timer}

class TimerBatcher[Req, Rep](duration: Duration, timer: Timer) extends Batcher[Req, Rep] {
  val mkService = (factory: ServiceFactory[Seq[Req], Seq[Rep]]) => new Service[Req, Rep] {
    @volatile var stopped = false
    var seq = Seq.empty[(Req, Promise[Rep])]

    def apply(req: Req): Future[Rep] = {
      val p = Promise[Rep]
      if (stopped) {
        return Future.exception(new Exception("failed"))
      }
      synchronized {
        seq :+= (req, p)
      }
      p
    }

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

    def loop() {
      timer.schedule(duration) {
        val old = synchronized {
          val tmp = seq
          seq = Seq.empty[(Req, Promise[Rep])]
          tmp
        }
        val svc = factory.toService
        Await.result(fulfilBatch(svc, old))
        if (!stopped) {
          loop()
        }
      }
    }

    loop()
  }

  override def apply(factory: ServiceFactory[Seq[Req], Seq[Rep]]): ServiceFactory[Req, Rep] = ServiceFactory.const(mkService(factory))
}
