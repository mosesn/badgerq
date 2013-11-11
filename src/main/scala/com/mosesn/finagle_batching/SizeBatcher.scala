package com.mosesn.finagle_batching

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Duration, Future, Promise, Time, Timer}

class SizeBatcher[Req, Rep](size: Int) extends Batcher[Req, Rep] {
  override def apply(factory: ServiceFactory[Seq[Req], Seq[Rep]]): ServiceFactory[Req, Rep] = ServiceFactory.const(mkService(factory))
}

class SizeBatchingService(
  factory: ServiceFactory[Seq[Req], Seq[Rep]],
  size: Int
) extends BatchingService[Req, Rep](factory) {
  var seq = Seq.empty[(Req, Promise[Rep])]

  def injectSeq(req: Req): Promise[Rep] = {
    val p = Promise[Rep]
    val oldMaybe = synchronized {
      seq :+= (req, p)
      if (seq.size > size) {
        val tmp = seq

        seq = Seq.empty[(Req, Promise[Rep])]
        Some(tmp)
      } else {
        None
      }
    }
    oldMaybe foreach { old =>
      val svc = factory.toService
      fulfilBatch(svc, old) transform { _ => p }
    }
    p
  }
}

