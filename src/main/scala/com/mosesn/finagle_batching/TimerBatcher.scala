package com.mosesn.finagle_batching

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Await, Duration, Future, Promise, Time, Timer}

class TimerBatcher[Req, Rep](duration: Duration, timer: Timer) extends Batcher[Req, Rep] {
  override def apply(factory: ServiceFactory[Seq[Req], Seq[Rep]]): ServiceFactory[Req, Rep] =
    ServiceFactory.const(new TimerBatchingService(factory))
}

class TimerBatchingService[Req, Rep](
  factory: ServiceFactory[Seq[Req], Seq[Rep]],
  duration: Duration,
  timer: Timer
) extends BatchingService[Req, Rep](factory) {
  var seq = Seq.empty[(Req, Promise[Rep])]

  def injectSeq(req: Req): Future[Rep] = {
    val p = Promise[Rep]
    synchronized {
      seq :+= (req, p)
    }
    p
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
