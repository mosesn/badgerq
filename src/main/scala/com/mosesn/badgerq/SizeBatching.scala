package com.mosesn.badgerq

import com.mosesn.pennsylvania.State
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Await, Duration, Future, Promise, Time, Timer}

class SizeBatching(size: Int) extends QueueingDiscipline {
  private[this] var curSize = 0

  def onProduce(f: Future[Unit]) {
    synchronized {
      curSize += 1
      if (curSize >= size) {
        Await.result(state.send(Ready))
      }
    }
  }

  def onConsume(f: Future[Unit]) {
    synchronized {
      curSize = 0
    }
  }

  val state: State[Status] = Status.default
}
