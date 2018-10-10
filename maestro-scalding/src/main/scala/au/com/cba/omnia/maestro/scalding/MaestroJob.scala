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

import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.NonFatal

import com.twitter.scalding.{Config, Mode, Execution, ExecutionApp}

/** An enumeration representing possible program status */
sealed trait JobStatus {
  def exitCode: Int
}

/** The job succeeded and had work to do */
case object JobFinished extends JobStatus { val exitCode = 0 }

/** The job is not ready to run: it's pre-requisites are not available */
case object JobNotReady extends JobStatus { val exitCode = 1 }

/** The Job was never ready, and could not be retried any more */
case object JobNeverReady extends JobStatus { val exitCode = 2 }

/** The Job was not scheduled to run, and should not be retried */
case object JobNotScheduled extends JobStatus { val exitCode = 3 }

/**
  * The job failed
  *
  * The exit code has to be negative.
  */
case class JobFailure(override val exitCode: Int) extends JobStatus {
  require(exitCode < 0, s"The exit code for a job failure must be < 0. Got $exitCode")
}

/**
  * Exception representation of the [[JobNotReady]] status.
  *
  * This is used so that we can represent these statuses as errors that Execution understands.
  * A better way would be to create a proper wrapper with Execution but this is a more lightweight
  * bridging solution.
  */
case object JobNotReadyException extends Exception("Job not ready")

/**
  * Exception representation of the [[JobFailureStatus]] status.
  *
  * This is used so that we can represent these statuses as errors that Execution understands.
  * A better way would be to create a proper wrapper with Execution but this is a more lightweight
  * bridging solution.
  */
case class JobFailureException(exitCode: Int) extends Exception(s"Job failure with exit code $exitCode")

/** Companion object for JobFailure to create a default JobFailure. */
object JobFailure {
  /** Creates a default job failure with exit code -1. */
  def apply(): JobFailure = new JobFailure(-1)
}

/**
  * Create an object which extends this trait to create a main class using the execution api.
  *
  * Objects extending this trait must supply a:
  *  - job execution: this is the main execution to run for the Maestro job
  *  - attemptsExceeded execution: this execution should be run if the job execution fails too many times.
  *  - logger: This will be used to log error messages from the above two executions.
  *
  * The job will exit with an error code describing the status of the program.
  * The software running this program can then take the appropriate action.
  */
trait MaestroJob extends Serializable {
  /** The job to run */
  def job: Execution[JobStatus]

  /** Run if the job has failed too many times */
  def attemptsExceeded: Execution[JobStatus]

  /** The logger to use for this application */
  def logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    val status = try {
      val (conf, mode) = ExApp.config(args)
      val execution    = if (conf.getArgs.boolean("attempts-exceeded")) attemptsExceeded else job
      MaestroExecution.recoverJobStatus(execution).waitFor(conf, mode).get
    } catch {
      case NonFatal(ex) => {
        logger.error("error running execution", ex)
        JobFailure(-1)
      }
    }
    System.exit(status.exitCode)
  }

  private object ExApp extends ExecutionApp {
    val CounterTimeoutConfig = ("cascading.step.counter.timeout", "60") //Sufficiently large

    def job = null // never used

    override def config(inputArgs: Array[String]): (Config, Mode) = {
      val (config, mode) = super.config(inputArgs)
      (config + CounterTimeoutConfig, mode)
    }
  }
}
