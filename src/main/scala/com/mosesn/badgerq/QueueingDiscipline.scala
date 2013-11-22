package com.mosesn.badgerq

import com.twitter.util.{Closable, Extractable, Updatable, Var}

trait QueueingDiscipline extends Closable {
  final def or(other: QueueingDiscipline): QueueingDiscipline =
    QueueingDiscipline.or(this, other)

  final def and(other: QueueingDiscipline): QueueingDiscipline =
    QueueingDiscipline.and(this, other)

  def onConsume()

  def onProduce()

  val state: Var[State] with Extractable[State] with Updatable[State] = Var(Pending)
}

trait QueueingDisciplines extends QueueingDiscipline {
  protected[this] val disciplines: Seq[QueueingDiscipline]
  final def onConsume() {
    disciplines foreach { _.onConsume() }
  }

  final def onProduce() {
    disciplines foreach { _.onProduce() }
  }
}

sealed trait State

case object Pending extends State
case object Ready extends State
case object Running extends State
case object Stopped extends State

object QueueingDiscipline {
  def and(disciplines: QueueingDiscipline*): QueueingDiscipline =
    new ConjunctiveDiscipline(disciplines)

  def or(disciplines: QueueingDiscipline*): QueueingDiscipline =
    new DisjunctiveDiscipline(disciplines)
}
