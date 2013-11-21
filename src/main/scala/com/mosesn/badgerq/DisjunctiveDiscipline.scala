package com.mosesn.badgerq

import com.twitter.util.{Future, Extractable, Updatable, Closable, Time}

class DisjunctiveDiscipline(
  protected[this] val disciplines: Seq[QueueingDiscipline]
) extends QueueingDisciplines {
  private[this] val observations: Seq[Closable] = (disciplines map { discipline =>
    discipline.state observe { case state =>
      state synchronized {
        transition(state, discipline.state)
      }
    }
  }) :+ (state observe {
    case Pending => {
      disciplines foreach { _.state() = Pending }
    }
    case _ =>
  })

  private[this] def transition(s: State, vari: Updatable[State] with Extractable[State]) {
    if (s == vari() && !(state() == Stopped)) { // did another thread win
      s match {
        case Ready =>
          vari() = Running // signals to itself that it won
          state() = Running // should reset itself
          disciplines foreach { _.state() = Pending }
        case _ =>
      }
    }
  }

  def close(deadline: Time): Future[Unit] = {
    state() = Stopped
    val tmp: Seq[Closable] = disciplines
    Closable.all(observations ++ tmp: _*).close(deadline)
  }
}
