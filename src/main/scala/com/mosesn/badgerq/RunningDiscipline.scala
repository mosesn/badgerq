package com.mosesn.badgerq

import com.mosesn.pennsylvania.{GoesTo, Rule, StartsAt, State, Transition}
import com.twitter.util.{Await, Closable, Future, Time}

class RunningDiscipline(self: QueueingDiscipline) extends QueueingDiscipline {
  def onConsume(f: Future[Unit]) {
    self.onConsume(f)
  }

  def onProduce(f: Future[Unit]) {
    self.onProduce(f)
  }

  val observer = self.state.state.observe {
    case Ready => {
      Await.result(state.send(Running))
    }
    case _ =>
  }

  override val state: State[Status] = {
    val synchronize: Status => Unit = { s: Status =>
      self.state.send(s)
    }
    State.mk[Status](Seq[Rule[Status]](
      new StartsAt(Pending),
      new Transition(Pending, Set(Running), synchronize),
      new Transition(Running, Set(Pending), synchronize),
      new GoesTo(Pending, Stopped),
      new GoesTo(Running, Stopped)
    ))
  }

  override def close(deadline: Time): Future[Unit] =
    super.close(deadline) flatMap { _ => Closable.all(observer, self).close(deadline) }
}
