package com.mosesn.badgerq

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util._
import scala.collection.immutable.Queue

class BatchingService[Req, Rep](
  factory: ServiceFactory[Seq[Req], Seq[Rep]],
  discipline: QueueingDiscipline
) extends Service[Req, Rep] {
  private[this] var q = Queue.empty[(Req, Promise[Rep])]
  val svc = factory.toService

  private[this] val observation = discipline.state observe {
    case Ready =>
      discipline.state() = Running
    case Running => {
      consume()
      discipline.state() = Pending
    }
    case _ =>
  }

  def apply(req: Req): Future[Rep] = {
    if (discipline.state() == Stopped) {
      return Future.exception(new Exception("failed"))
    }
    produce(req)
  }

  def produce(req: Req): Future[Rep] = {
    val p = Promise[Rep]
    synchronized {
      q :+= (req, p)
      discipline.onProduce(p.unit)
    }
    p
  }

  override def close(deadline: Time): Future[Unit] = {
    Closable.all(discipline, observation).close(deadline)
  }

  def consume() {
    synchronized {
      fulfilBatch(q)
      q = Queue.empty[(Req, Promise[Rep])]
    }
  }

  def fulfilBatch(pairs: Seq[(Req, Promise[Rep])]): Future[Unit] = {
    val p = Promise[Unit]()
    discipline.onConsume(p)
    val f = (svc(pairs map (_._1)) onSuccess { results =>
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
    p.become(f)
    p
  }
}
