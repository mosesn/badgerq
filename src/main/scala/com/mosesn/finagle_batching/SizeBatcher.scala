package com.mosesn.finagle_batching

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Duration, Future, Promise, Time, Timer}

class SizeBatching(size: Int) extends QueueingDiscipline {
  private[this] var curSize = 0

  def onProduce() {
    synchronized {
      curSize += 1
      if (curSize >= size) {
        state() = Ready
      }
    }
  }

  def onConsume() {
    synchronized {
      curSize = 0
    }
  }
}

