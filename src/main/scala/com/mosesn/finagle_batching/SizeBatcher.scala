package com.mosesn.finagle_batching

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Duration, Future, Promise, Time, Timer}

class SizeBatcher[Req, Rep](size: Int) extends Batcher[Req, Rep] {

  val mkService = (factory: ServiceFactory[Seq[Req], Seq[Rep]]) => new Service[Req, Rep] {
    @volatile var stopped = false
    var seq = Seq.empty[(Req, Promise[Rep])]

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

    def apply(req: Req): Future[Rep] = {
      if (stopped) {
        return Future.exception(new Exception("failed"))
      }
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

    override def close(deadline: Time): Future[Unit] = {
      stopped = true
      Future.Done
    }
  }

  override def apply(factory: ServiceFactory[Seq[Req], Seq[Rep]]): ServiceFactory[Req, Rep] = ServiceFactory.const(mkService(factory))
}
