package com.mosesn.finagle_batching

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util._
import scala.collection.immutable.Queue

class BatchingService[Req, Rep](
  factory: ServiceFactory[Seq[Req], Seq[Rep]],
  disciplines: Seq[QueueingDiscipline]
) extends Service[Req, Rep] {
  private[this] var q = Queue.empty[(Req, Promise[Rep])]
  val svc = factory.toService

  val queueState: Var[State] with Updatable[State] with Extractable[State] = Var(Pending)

  queueState observe {
    case Running => {
      consume()
      queueState() = Pending
    }
    case _ =>
  }

  private[this] val observations = disciplines map { discipline =>
    discipline.state observe { case state =>
      queueState synchronized {
        transition(state, discipline.state)
      }
    }
  }

  private[this] def transition(state: State, vari: Updatable[State] with Extractable[State]) {
    if (state == vari() && queueState() != Stopped) { // did another thread win
      state match {
        case Ready =>
          vari() = Running // signals to itself that it won
          queueState() = Running // should reset itself
          disciplines foreach { _.state() = Pending }
        case _ =>
      }
    }
  }

  def apply(req: Req): Future[Rep] = {
    if (queueState() == Stopped) {
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
    queueState() = Stopped // must be synchronized
    Closable.all(observations: _*).close(deadline)
  }

  def consume() {
    synchronized {
      fulfilBatch(q)
      q = Queue.empty[(Req, Promise[Rep])]
    }
  }

  def fulfilBatch(pairs: Seq[(Req, Promise[Rep])]): Future[Unit] = {
    disciplines foreach { _.onConsume() }
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
