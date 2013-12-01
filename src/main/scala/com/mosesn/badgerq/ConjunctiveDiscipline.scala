package com.mosesn.badgerq

import com.mosesn.pennsylvania.State
import com.twitter.util.{Await, Closable, Extractable, Future, Time, Var}

class ConjunctiveDiscipline(unrolledDisciplines: Seq[QueueingDiscipline]) extends QueueingDisciplines {
  // this has max depth of ~5, because seq is bound by Int.MaxValue
  protected[this] val disciplines = {
    val size = unrolledDisciplines.size
    if (size > 64) {
      // bundles to groups of size < 64
      val bundleSize = 64 max ((size / 64) + 1)
      (unrolledDisciplines.grouped(bundleSize) map {
        new ConjunctiveDiscipline(_)
      }).toSeq
    } else {
      unrolledDisciplines
    }
  }

  @volatile private[this] var initiator: State[Status] = null

  private[this] val mask = -1 >>> (64 - disciplines.size)
  private[this] var bitstring = 0L

  // NB: this only works for up to 64, fine because of bundling
  private[this] val observations = (disciplines.zipWithIndex map { case (discipline, idx) =>
    discipline.state.state observe {
      case Ready => synchronized {
        bitstring |= (1L << idx)
        if (bitstring == mask) {
          initiator = discipline.state
          Await.result(state.send(Ready))
        }
      }
      case cur => synchronized {
        // for idempotence, might be able to do better with careful thought
        bitstring &= (-1L - (1L << idx))
        if (state.state() != cur) {
          Await.result(state.send(cur))
        }
      }
    }
  }) :+ (state.state observe {
    case Running => Await.result(initiator.send(Running))
    case Pending => disciplines foreach { d =>
      Await.result(d.state.send(Pending)) }
    case _ =>
  })

  override def close(deadline: Time): Future[Unit] =
    super.close(deadline) flatMap { _ => Closable.all(observations ++ disciplines: _*).close() }
}
