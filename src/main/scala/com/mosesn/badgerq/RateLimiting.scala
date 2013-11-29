package com.mosesn.badgerq

import com.twitter.util.{Future, Time}
import com.twitter.concurrent.AsyncSemaphore

class RateLimiting(num: Int) extends QueueingDisciplineProxy(
  new RawRateLimiting(num) and new SizeBatching(1)
)

private[badgerq] class RawRateLimiting(num: Int) extends QueueingDiscipline {
  require(num > 0)

  private[this] var cur = 0

  val sem = new AsyncSemaphore(num)

  def onConsume(f: Future[Unit]) {
    synchronized {
      cur += 1
      if (cur == num) {
        state() = Pending
      }
    }
    f ensure {
      synchronized {
        cur -= 1
        if (cur < num && state() == Pending) {
          state() = Ready
        }
      }
    }
  }

  def onProduce(f: Future[Unit]) {
    if (cur < num && state() == Pending) {
      state() = Ready
    }
  }

  override def close(deadline: Time): Future[Unit] = synchronized {
    state() = Stopped
    Future.Done
  }
}
