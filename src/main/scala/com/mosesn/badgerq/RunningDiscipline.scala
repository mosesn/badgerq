package com.mosesn.badgerq

import com.twitter.util.Await

class RunningDiscipline(self: QueueingDiscipline) extends QueueingDisciplineProxy(self) {
  self.state.state.observe {
    case Ready => Await.result(self.state.send(Running))
    case _ =>
  }
}
