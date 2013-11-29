package com.mosesn.badgerq

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Duration, Future, Promise, Time, Timer}

class SizeBatching(size: Int) extends QueueingDiscipline {
  private[this] var curSize = 0

  def onProduce(f: Future[Unit]) {
    synchronized {
      curSize += 1
      if (curSize >= size) {
        state() = Ready
      }
    }
  }

  def onConsume(f: Future[Unit]) {
    synchronized {
      curSize = 0
    }
  }

  def close(deadline: Time): Future[Unit] = {
    state() = Stopped
    Future.Done
  }
}
