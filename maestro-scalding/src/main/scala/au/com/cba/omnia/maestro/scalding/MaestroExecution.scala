//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.maestro.scalding

import com.twitter.scalding.Execution

/** Maestro specific Execution functions. */
object MaestroExecution {
  /** Creates an execution that has a [[JobNotReady]] error. */
  def jobNotReady[T]: Execution[T] = Execution.from[T](throw JobNotReadyException)

  /** Creates an execution that has a [[JobFailure]] error. */
  def jobFailure[T](exitCode: Int = -1): Execution[T] =
    Execution.from[T](throw new JobFailureException(exitCode))

  /** Lifts a [[JobNotReady]] or [[JobFailure]] error into a [[JobStatus]] value for execution.
    *
    * Due to the way that Omnitool embeds [[Result]] failures into [[Execution]], [[ResultException]]
    * is checked (see `resultToFuture` in [[RichExecution]]).
    */
  def recoverJobStatus(execution: Execution[JobStatus]): Execution[JobStatus] =
    execution.recoverWith {
      case JobNotReadyException                                       => Execution.from(JobNotReady)
      case ResultException(_, _, Some(JobNotReadyException))          => Execution.from(JobNotReady)
      case JobFailureException(exitCode)                              => Execution.from(JobFailure(exitCode))
      case ResultException(_, _, Some(JobFailureException(exitCode))) => Execution.from(JobFailure(exitCode))
    }

  /** Iff the condition is not true changes the status of the Execution to JobNotReady. */
  def guardReady(check: Boolean): Execution[Unit] = if (check) Execution.from(()) else jobNotReady
}
