package com.mosesn.badgerq

import com.twitter.util.{Closable, Extractable, Future, Time, Updatable, Var}
import com.mosesn.pennsylvania.{GoesTo, Rule, State}

trait QueueingDiscipline extends Closable {
  final def or(other: QueueingDiscipline): QueueingDiscipline =
    QueueingDiscipline.or(this, other)

  final def and(other: QueueingDiscipline): QueueingDiscipline =
    QueueingDiscipline.and(this, other)

  private[this] lazy val opposite = new NegativeDiscipline(this)
  final def not: QueueingDiscipline = opposite

  def onConsume(f: Future[Unit])

  def onProduce(f: Future[Unit])

  val state: State[Status]

  def close(deadline: Time): Future[Unit] = {
    state.send(Stopped) flatMap {
      case true => Future.Done
      case false => close(deadline) // TODO: maybe a less shitty spinlock . . .
    }
  }
}

object QueueingDiscipline {
  def and(disciplines: QueueingDiscipline*): QueueingDiscipline =
    new ConjunctiveDiscipline(disciplines)

  def or(disciplines: QueueingDiscipline*): QueueingDiscipline =
    new DisjunctiveDiscipline(disciplines)
}

class QueueingDisciplineProxy(self: QueueingDiscipline) extends QueueingDiscipline {
  def onConsume(f: Future[Unit]) {
    self.onConsume(f)
  }

  def onProduce(f: Future[Unit]) {
    self.onProduce(f)
  }

  override def close(deadline: Time): Future[Unit] = self.close(deadline)

  val state: State[Status] = self.state
}

trait QueueingDisciplines extends QueueingDiscipline {
  protected[this] val disciplines: Seq[QueueingDiscipline]
  final def onConsume(f: Future[Unit]) {
    disciplines foreach { _.onConsume(f) }
  }

  final def onProduce(f: Future[Unit]) {
    disciplines foreach { _.onProduce(f) }
  }

  val state: State[Status] = Status.default()
}

sealed trait Status

object Status {
  private[this] val rules = Rule.consolidate[Status](Seq(
    new GoesTo(Set[Status](null), Pending),
    new GoesTo(Pending, Set[Status](Ready, Stopped)),
    new GoesTo(Ready, Set[Status](Pending, Running, Stopped)),
    new GoesTo(Running, Pending)
  ))

  def default(): State[Status] = State.mk(rules)
}

case object Pending extends Status
case object Ready extends Status
case object Running extends Status
case object Stopped extends Status
