package com.mosesn.badgerq

import com.twitter.util.{Closable, Extractable, Future, Time, Updatable, Var}

trait QueueingDiscipline extends Closable {
  final def or(other: QueueingDiscipline): QueueingDiscipline =
    QueueingDiscipline.or(this, other)

  final def and(other: QueueingDiscipline): QueueingDiscipline =
    QueueingDiscipline.and(this, other)

  private[this] lazy val opposite = new NegativeDiscipline(this)
  final def not: QueueingDiscipline = opposite

  def onConsume(f: Future[Unit])

  def onProduce(f: Future[Unit])

  val state: Var[State] with Extractable[State] with Updatable[State] = Var(Pending)
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

  def close(deadline: Time): Future[Unit] = self.close(deadline)

  override val state: Var[State] with Extractable[State] with Updatable[State] = self.state
}

trait QueueingDisciplines extends QueueingDiscipline {
  protected[this] val disciplines: Seq[QueueingDiscipline]
  final def onConsume(f: Future[Unit]) {
    disciplines foreach { _.onConsume(f) }
  }

  final def onProduce(f: Future[Unit]) {
    disciplines foreach { _.onProduce(f) }
  }
}

sealed trait State

case object Pending extends State
case object Ready extends State
case object Running extends State
case object Stopped extends State
