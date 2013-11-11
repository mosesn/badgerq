package com.mosesn

import com.twitter.finagle.ServiceFactory

package object finagle_batching {
  type Batcher[Req, Rep] = ServiceFactory[Seq[Req], Seq[Rep]] => ServiceFactory[Req, Rep]
}
