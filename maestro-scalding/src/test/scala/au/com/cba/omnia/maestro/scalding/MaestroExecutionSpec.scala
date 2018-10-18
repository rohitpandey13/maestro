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

import au.com.cba.omnia.omnitool.Result

import au.com.cba.omnia.thermometer.core.ThermometerSpec

import ExecutionOps._

object MaestroExecutionSpec extends ThermometerSpec { def is = s2"""

Maestro Execution functions
===========================

Maestro Execution functions:
  can recover `JobNotReadyError`                             $jobNotReady
  can recover `JobFailureError`                              $jobFailure
  can recover `JobNotReadyError` wrapped in Result exception $jobNotReadyResultException
  can recover `JobFailureError` wrapped in Result exception  $jobFailureResultException

"""

  def jobNotReady = {
    executesSuccessfully(MaestroExecution.recoverJobStatus(MaestroExecution.jobNotReady)) must_== JobNotReady
  }

  def jobFailure = {
    executesSuccessfully(MaestroExecution.recoverJobStatus(MaestroExecution.jobFailure(-2))) must_== JobFailure(-2)
  }

  def jobNotReadyResultException = {
    val exec = Execution.fromResult(Result.safe(throw JobNotReadyException))
    executesSuccessfully(MaestroExecution.recoverJobStatus(exec)) must_== JobNotReady
  }

  def jobFailureResultException = {
    val exec = Execution.fromResult(Result.safe(throw JobFailureException(-2)))
    executesSuccessfully(MaestroExecution.recoverJobStatus(exec)) must_== JobFailure(-2)
  }
}
