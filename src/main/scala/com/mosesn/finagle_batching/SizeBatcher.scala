package com.mosesn.finagle_batching

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Duration, Future, Promise, Time, Timer}

class SizeBatcher[Req, Rep](size: Int) extends Batcher[Req, Rep] {
  override def apply(factory: ServiceFactory[Seq[Req], Seq[Rep]]): ServiceFactory[Req, Rep] = ServiceFactory.const(new SizeBatchingService(size, factory))
}

class SizeBatchingService[Req, Rep](val size: Int, factory: ServiceFactory[Req, Rep]) extends BatchingService[Req, Rep] with SizeBatching

trait SizeBatching extends QueueingDiscipline {
  val size: Int

  var curSize = 0

  def onProduce() {
    synchronized {
      curSize += 1
      if (curSize > size) {
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
