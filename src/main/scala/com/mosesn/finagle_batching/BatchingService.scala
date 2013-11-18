package com.mosesn.finagle_batching

import com.twitter.finagle.Service
import com.twitter.util.{Await, Duration, Future, Promise, Time, Timer}

abstract class BatchingService[Req, Rep](
  factory: ServiceFactory[Seq[Req], Seq[Rep]],
  disciplines: Seq[QueueingDiscipline]
) extends Service[Req, Rep] {
  private[this] var q = Queue.empty[(Req, Promise[Rep])]
  val svc = factory.toService

  val queueState = Var(Pending)

  queueState observe {
    case Running =>
      consume()
      queueState() = Pending
    case _ =>
  }

  val observations = disciplines yield { discipline =>
    discipline.state observe { case state =>
      queueState synchronized {
        state match {
          case Ready if queueState() == Running =>
            discipline.state() = Interrupted
          case Ready =>
            discipline.state() = Running
            queueState() = Running
          case _ =>
        }
      }
    }
  }

  def apply(req: Req): Future[Rep] = {
    if (stopped) {
      return Future.exception(new Exception("failed"))
    }
    produce(req)
  }

  def produce(req: Req): Future[Rep] = {
    val p = Promise[Rep]
    synchronized {
      q :+= (req, p)
      disciplines foreach { _.onProduce() }
    }
    p
  }

  override def close(deadline: Time): Future[Unit] = {
    stopped = true
    Future.Done
  }

  def consume() {
    synchronized {
      fulfilBatch(q)
      q = Queue.empty[(Req, Promise[Rep])]
    }
  }

  def fulfilBatch(pairs: Seq[(Req, Promise[Rep])]): Future[Unit] = {
    consumeHandlers foreach (_())
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
