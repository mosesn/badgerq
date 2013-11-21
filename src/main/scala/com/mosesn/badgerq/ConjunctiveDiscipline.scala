package com.mosesn.badgerq

import com.twitter.util.{Await, Closable, Extractable, Future, Time, Updatable, Var}

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

  @volatile private[this] var initiator: Updatable[State] = null

  private[this] val mask = Long.MinValue >>> (64 - disciplines.size)
  private[this] var bitstring = 0L

  // NB: this only works for up to 64, fine because of bundling
  private[this] val observations = (disciplines.zipWithIndex map { case (discipline, idx) =>
    discipline.state observe {
      case Ready => synchronized {
        bitstring |= (1L << idx)
        if (bitstring == mask) {
          initiator = discipline.state
          state() = Ready
        }
      }
      case _ => synchronized {
        // for idempotence, might be able to do better with careful thought
        bitstring &= (-1L - (1L << idx))
      }
    }
  }) :+ (state observe {
    case Running => initiator() = Running
    case Pending => {
      disciplines foreach { _.state() = Pending }
    }
    case _ =>
  })

  def close(deadline: Time): Future[Unit] = {
    state() = Stopped
    Closable.all(observations ++ disciplines: _*).close()
  }
}
