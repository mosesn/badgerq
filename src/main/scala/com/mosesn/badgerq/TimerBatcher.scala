package com.mosesn.badgerq

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Await, Duration, Future, Promise, Time, Timer, TimerTask}

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

  def onConsume() {
    if (state() != Running) {
      synchronized {
        task foreach { _.cancel() }
      }
    }
    task = None
    loop()
  }

  loop()
}
