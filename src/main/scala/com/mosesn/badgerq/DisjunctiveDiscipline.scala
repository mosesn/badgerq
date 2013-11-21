package com.mosesn.badgerq

import com.twitter.util.{Future, Extractable, Updatable, Closable, Time}

class DisjunctiveDiscipline(
  protected[this] val disciplines: Seq[QueueingDiscipline]
) extends QueueingDisciplines {
  @volatile private[this] var initiator: Updatable[State] = null

  private[this] val observations: Seq[Closable] = (disciplines map { discipline =>
    discipline.state observe { case s =>
      transition(s, discipline.state)
    }
  }) :+ (state observe {
    case Running => {
      initiator() = Running
    }
    case Pending => {
      disciplines foreach { _.state() = Pending }
    }
    case _ =>
  })

  private[this] def transition(s: State, vari: Updatable[State] with Extractable[State]) {
    s match {
      case Ready =>
        synchronized {
          if (!(state() == Ready || state() == Running)) { // did another thread win
            initiator = vari
            state() = Ready
          }
        }
      case _ =>
    }
  }

  def close(deadline: Time): Future[Unit] = {
    state() = Stopped
    val tmp: Seq[Closable] = disciplines
    Closable.all(observations ++ tmp: _*).close(deadline)
  }
}
