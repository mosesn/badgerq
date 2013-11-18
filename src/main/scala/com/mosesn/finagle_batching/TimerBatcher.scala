package com.mosesn.finagle_batching

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Await, Duration, Future, Promise, Time, Timer}

class TimerBatcher[Req, Rep](duration: Duration, timer: Timer) extends Batcher[Req, Rep] {
  override def apply(factory: ServiceFactory[Seq[Req], Seq[Rep]]): ServiceFactory[Req, Rep] =
    ServiceFactory.const(new TimerBatchingService(duration, timer, factory))
}

class TimerBatchingService[Req, Rep](
  val duration: Duration,
  val timer: Timer,
  factory: ServiceFactory[Seq[Req, Seq[Rep]]]
) extends BatchingService[Req, Rep](factory) with TimerBatching

trait TimerBatching[Req, Rep] { self: BatchingService =>
  val duration: Duration
  val timer: Timer

  @volatile var reset = false
  @volatile var running = false
  @volatile var task: Option[TimerTask] = None

  def loop() {
    if (stopped) {
      task = None
    } else {
      task = Some(timer.schedule(duration) {
        val old = synchronized {
          running = true
          if (!reset) {
            val tmp = q
            q = Queue.empty[(Req, Promise[Rep])]
            tmp
            val svc = factory.toService
            fulfilBatch(svc, old)
          }
        }

        if (!reset) {
          running = false
        }
        reset = false
      })
    }
  }

  onConsume {
    if (!running) {
      task.cancel()
      reset = true
      loop()
    }
  }

  loop()
}
