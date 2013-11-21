package com.mosesn.badgerq

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Await, Closable, Duration, Future, Promise, Time, Timer, TimerTask}

class TimerBatching(duration: Duration, timer: Timer) extends QueueingDiscipline {
  @volatile var task: Option[TimerTask] = None

  def loop() {
    if (state() == Stopped) {
      task = None
    } else {
      task = Some(timer.schedule(duration) {
        state() = Ready
      })
    }
  }

  def onProduce() {}

  // FIXME: this is broken until running semantics are OK
  def onConsume() {
    if (!(state() == Running)) {
      synchronized {
        task foreach { _.cancel() }
      }
    }
    task = None
    loop()
  }

  def close(deadline: Time): Future[Unit] = {
    state() = Stopped
    val prev = task
    task = None
    Closable.all(prev.toSeq: _*).close(deadline)
  }

  // FIXME: loop should start on production
  // maybe use an atomic reference around the task?
  // could also use an atomic boolean if eagerness around the task makes it complicated
  loop()
}
