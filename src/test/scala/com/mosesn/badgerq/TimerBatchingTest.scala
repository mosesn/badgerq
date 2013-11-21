package com.mosesn.badgerq

import com.twitter.conversions.time.intToTimeableNumber
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Await, Future, MockTimer, Time}
import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

class TimerBatchingTest extends FunSpec with ShouldMatchers {
  trait TimerBatchingHelper {
    val timer = new MockTimer
    val fac = ServiceFactory.const(Service.mk { seq: Seq[Int] => Future.value(seq) })
    val svc = new BatchingService(fac, new TimerBatching(1.second, timer))
  }

  trait MultiTimerBatchingHelper {
    val timer = new MockTimer
    val fac = ServiceFactory.const(Service.mk { seq: Seq[Int] => Future.value(seq) })
    val svc = new BatchingService(
      fac,
      new TimerBatching(1.second, timer) or new TimerBatching(1.second + 500.milliseconds, timer)
    )
  }

  describe("TimerBatching") {
    it("should not batch before the timeout") {
      new TimerBatchingHelper {
        Time.withCurrentTimeFrozen { ctl =>
          val f = svc(0)
          ctl.advance(500.milliseconds)
          timer.tick()
          f should not be ('defined)
        }
      }
    }

    it("should batch on the timeout") {
      new TimerBatchingHelper {
        Time.withCurrentTimeFrozen { ctl =>
          val f = svc(0)
          ctl.advance(1.second)
          timer.tick()
          Await.result(f) should be (0)
        }
      }
    }

    it("should batch a few items properly") {
      new TimerBatchingHelper {
        Time.withCurrentTimeFrozen { ctl =>
          val f1 = svc(0)
          val f2 = svc(1)
          ctl.advance(1.second)
          timer.tick()
          Await.result(f1) should be (0)
          Await.result(f2) should be (1)
        }
      }
    }

    it("should not batch after consuming after the timeout") {
      new TimerBatchingHelper {
        Time.withCurrentTimeFrozen { ctl =>
          val f1 = svc(0)
          ctl.advance(1.second)
          timer.tick()
          Await.result(f1) should be (0)

          val f2 = svc(0)
          ctl.advance(500.milliseconds)
          timer.tick()
          f2 should not be ('defined)
        }
      }
    }

    it("should batch a few times") {
      new TimerBatchingHelper {
        Time.withCurrentTimeFrozen { ctl =>
          val f1 = svc(0)
          ctl.advance(1.second)
          timer.tick()
          Await.result(f1) should be (0)

          val f2 = svc(0)
          ctl.advance(1.second)
          timer.tick()
          Await.result(f2) should be (0)
        }
      }
    }

    it("should only respect the first timeout") {
      new MultiTimerBatchingHelper {
        Time.withCurrentTimeFrozen { ctl =>
          val f1 = svc(0)
          ctl.advance(1.second)
          timer.tick()
          Await.result(f1) should be (0)

          val f2 = svc(0)
          ctl.advance(500.milliseconds)
          timer.tick()
          f2 should not be ('defined)
        }
      }
    }
  }
}
