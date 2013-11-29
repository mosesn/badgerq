package com.mosesn.badgerq

import com.twitter.util.{Closable, Future, Time}

class NegativeDiscipline(underlying: QueueingDiscipline) extends QueueingDiscipline {
  def onConsume(f: Future[Unit]) {
    underlying.onConsume(f)
  }

  def onProduce(f: Future[Unit]) {
    underlying.onProduce(f)
  }

  def close(deadline: Time): Future[Unit] = {
    Closable.all(closables :+ underlying: _*).close(deadline)
  }

  val closables = Seq(
    underlying.state observe {
      case Ready => if (state() != Ready) {
        state() = Pending
      }
      case Pending => if (state() != Ready) {
        state() = Ready
      }
      case _ =>
    },
    state observe {
      case Running => underlying.state() = Running
      case Stopped => underlying.state() = Stopped
      case Pending => if (underlying.state() != Ready) {
        underlying.state() = Ready
      }
      case _ =>
    }
  )
}
