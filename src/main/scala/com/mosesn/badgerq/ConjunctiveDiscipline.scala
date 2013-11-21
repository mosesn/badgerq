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

  private[this] val mask = Long.MinValue >>> (64 - disciplines.size)
  private[this] var bitstring = 0L

  // NB: this only works for up to 64, fine because of bundling
  private[this] val observations: Seq[Closable] = disciplines.zipWithIndex map { case (discipline, idx) =>
    discipline.state observe {
      case Ready => synchronized {
        bitstring |= (1L << idx)
        if (bitstring == mask) {
          state() = Ready
        }
      }
      case _ => synchronized {
        bitstring &= (-1L - (1L << idx))
      }
    }
  }

  private[this] val curObservation: Closable = state observe {
    case Pending => {
      disciplines foreach { _.state() = Pending }
    }
    case _ =>
  }

  def close(deadline: Time): Future[Unit] = {
    state() = Stopped
    Closable.all(observations ++ disciplines :+ curObservation: _*).close()
  }
}
