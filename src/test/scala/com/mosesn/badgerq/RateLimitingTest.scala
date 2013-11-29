package com.mosesn.badgerq

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Await, Future, Promise}
import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import scala.collection.immutable.Queue

class RateLimitingTest extends FunSpec with ShouldMatchers {
  trait RateLimitingHelper {
    var queue = Queue.empty[(Seq[Int], Promise[Seq[Int]])]
    val underlying = Service.mk[Seq[Int], Seq[Int]] { items: Seq[Int] =>
      val p = Promise[Seq[Int]]
      queue = queue :+ (items, p)
      p
    }
    val factory = ServiceFactory.const(underlying)

    val limiter = new RateLimiting(1)
    val svc = new BatchingService(factory, limiter)

    def fulfil() {
      val (item, promise) = queue.head
      queue = queue.tail
      promise.setValue(item)
    }

    def outstanding: Int = queue.size
  }

  describe("RateLimiting") {

    it("should batch if empty") {
      new RateLimitingHelper {
        val f = svc(0)
        assert(outstanding === 1)
        fulfil()
        assert(Await.result(f) === 0)
      }
    }

    it("should not batch if it's full") {
      new RateLimitingHelper {
        svc(0)
        svc(1)
        assert(outstanding === 1)
      }
    }

    it("should be able to go down from being full") {
      new RateLimitingHelper {
        val f1 = svc(0)
        assert(outstanding === 1)
        fulfil()
        assert(Await.result(f1) === 0)

        val f2 = svc(1)
        assert(outstanding === 1)
        fulfil()
        assert(Await.result(f2) === 1)
      }
    }

    it("should batch when first able to") {
      new RateLimitingHelper {
        val f1 = svc(0)
        val f2 = svc(1)

        assert(outstanding === 1)
        fulfil()
        assert(Await.result(f1) === 0)

        assert(outstanding === 1)
        fulfil()
        assert(Await.result(f2) === 1)
      }
    }

    it("should batch everything when able to") {
      new RateLimitingHelper {
        val f1 = svc(0)
        val f2 = svc(1)
        val f3 = svc(2)
        assert(outstanding === 1)
        fulfil()
        assert(Await.result(f1) === 0)

        assert(outstanding === 1)
        fulfil()
        assert(Await.result(f2) === 1)
        assert(Await.result(f3) === 2)
      }
    }
  }
}
