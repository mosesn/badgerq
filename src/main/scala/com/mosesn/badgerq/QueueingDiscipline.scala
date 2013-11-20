package com.mosesn.badgerq

import com.twitter.util.{Extractable, Updatable, Var}

trait QueueingDiscipline {
  def onConsume()

  def onProduce()

  val state: Var[State] with Extractable[State] with Updatable[State] = Var(Pending)
}

sealed trait State

case object Pending extends State
case object Ready extends State
case object Running extends State
case object Stopped extends State
