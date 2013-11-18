package com.mosesn.finagle_batching

trait QueueingDiscipline {
  def onConsume()

  def onProduce()

  val state: Var[State] with Updateable[State] with Extractable[State] =
    Var(Pending)
}

sealed trait State

case object Pending extends State
case object Running extends State
case object Interrupted extends State
case object Stopped extends State
