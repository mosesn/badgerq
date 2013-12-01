package com.mosesn.badgerq

import com.twitter.util.{Await, Closable, Future, Time}

class NegativeDiscipline(underlying: QueueingDiscipline) extends QueueingDiscipline {
  def onConsume(f: Future[Unit]) {
    underlying.onConsume(f)
  }

  def onProduce(f: Future[Unit]) {
    underlying.onProduce(f)
  }

  override def close(deadline: Time): Future[Unit] = super.close(deadline) flatMap { _ =>
    Closable.all(closables :+ underlying: _*).close(deadline)
  }

  val closables = Seq(
    underlying.state.state observe {
      case Ready => if (state.state() != Pending) {
        state.send(Pending)
      }
      case Pending => if (state.state() != Ready) {
        state.send(Ready)
      }
      case _ =>
    },
    state.state observe {
      case Ready =>
      case status => Await.result(underlying.state.send(Ready))
    }
  )

  val state = Status.default
}
