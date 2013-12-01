package com.mosesn.badgerq

import com.mosesn.pennsylvania.State
import com.twitter.util.{Await, Future, Extractable, Updatable, Closable, Time}

class DisjunctiveDiscipline(
  protected[this] val disciplines: Seq[QueueingDiscipline]
) extends QueueingDisciplines {
  @volatile private[this] var initiator: State[Status] = null

  private[this] val observations: Seq[Closable] = (disciplines map { discipline =>
    discipline.state.state observe { case s =>
      transition(s, discipline.state)
    }
  }) :+ (state.state observe {
    case Running => {
      Await.result(initiator.send(Running))
    }
    case Pending => {
      disciplines foreach { d => Await.result(d.state.send(Pending)) }
    }
    case _ =>
  })

  private[this] def transition(s: Status, vari: State[Status]) {
    s match {
      case Ready =>
        synchronized {
          if (!(state.state() == Ready || state.state() == Running)) { // did another thread win
            initiator = vari
            state.send(Ready)
          }
        }
      case _ =>
    }
  }

  override def close(deadline: Time): Future[Unit] = super.close(deadline) flatMap { _ =>
    Closable.all(observations ++ disciplines: _*).close(deadline)
  }
}
