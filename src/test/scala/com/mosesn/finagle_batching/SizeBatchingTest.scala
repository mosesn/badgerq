package com.mosesn.finagle_batching

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Await, Future}
import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

class SizeBatchingTest extends FunSpec with ShouldMatchers {
  trait SizeBatchingHelper {
    val fac = ServiceFactory.const(Service.mk { seq: Seq[Int] => Future.value(seq) })
    val svc = new BatchingService(fac, Seq(new SizeBatching(2)))
  }

  trait MultiSizeBatchingHelper {
    val fac = ServiceFactory.const(Service.mk { seq: Seq[Int] => Future.value(seq) })
    val svc = new BatchingService(fac, Seq(new SizeBatching(2), new SizeBatching(3)))
  }

  describe("SizeBatching") {
    it("should not batch before full") {
      new SizeBatchingHelper {
        val f1 = svc(3)
        f1 should not be ('defined)
      }
    }

    it("should batch after full") {
      new SizeBatchingHelper {
        val f1 = svc(3)
        f1 should not be ('defined)
        val f2 = svc(4)
        Await.result(f1) should be (3)
        Await.result(f2) should be (4)
      }
    }

    it("should not batch even after a successful batch") {
      new SizeBatchingHelper {
        val f1 = svc(3)
        f1 should not be ('defined)
        val f2 = svc(4)
        Await.result(f1) should be (3)
        Await.result(f2) should be (4)

        val f3 = svc(3)
        f3 should not be ('defined)
      }
    }

    it("should be able to do a few rounds") {
      new SizeBatchingHelper {
        val f1 = svc(3)
        f1 should not be ('defined)
        val f2 = svc(4)
        Await.result(f1) should be (3)
        Await.result(f2) should be (4)

        val f3 = svc(3)
        f3 should not be ('defined)
        val f4 = svc(4)
        Await.result(f3) should be (3)
        Await.result(f4) should be (4)
      }
    }

    it("should just use the first size batcher") {
      new MultiSizeBatchingHelper {
        val f1 = svc(3)
        f1 should not be ('defined)
        val f2 = svc(4)
        Await.result(f1) should be (3)
        Await.result(f2) should be (4)

        val f3 = svc(3)
        f3 should not be ('defined)
      }
    }
  }
}
