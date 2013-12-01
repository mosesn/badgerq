package com.mosesn.badgerq

import com.mosesn.pennsylvania.{State, Transition}
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Await, Closable, Duration, Future, Promise, Time, Timer, TimerTask}
import java.util.concurrent.atomic.AtomicBoolean

class TimerBatching(duration: Duration, timer: Timer) extends QueueingDiscipline {
  @volatile private[this] var task: Option[TimerTask] = None
  private[this] val bool = new AtomicBoolean()

  private[this] def mkTask() {
    if (state.state() == Stopped) {
      task = None
    } else {
      task = Some(timer.schedule(duration) {
        Await.result(state.send(Ready))
      })
    }
  }

  def onProduce(f: Future[Unit]) {
    if (bool.compareAndSet(false, true)) {
      mkTask()
    }
  }

  def onConsume(f: Future[Unit]) {
    task foreach { _.cancel() }
    bool.set(false)
    task = None
  }

  val state: State[Status] = State.mk { Status.rules :+ new Transition(Ready, Set[Status](Running), { s: Status =>
    task = None
  })}

  override def close(deadline: Time): Future[Unit] = super.close(deadline) flatMap { _ =>
    val prev = task
    task = None
    Closable.all(prev.toSeq: _*).close(deadline)
  }
}
