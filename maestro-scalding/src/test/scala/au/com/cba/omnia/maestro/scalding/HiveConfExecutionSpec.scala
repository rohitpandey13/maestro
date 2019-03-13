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

import org.apache.hadoop.mapred.JobConf

import com.twitter.scalding.Execution

import au.com.cba.omnia.beeswax.Hive

import au.com.cba.omnia.maestro.scalding.ConfHelper._
import au.com.cba.omnia.maestro.scalding.ExecutionOps._

import au.com.cba.omnia.thermometer.core.ThermometerSpec

object HiveConfExecutionSpec extends ThermometerSpec { def is = s2"""

Hive Conf Execution Spec
===========================

Hive Conf execution functions:
  can getHiveConf set the custom reducer based on job configured value                              $testGetHiveConfigCustomReducers
  can getHiveConf set default Hive reducer value if jobConf reducer value matches MR reducer value  $testGetHiveConfDefaultReducers
  can hive monad conf returns the custom reducer value based on job configured value                $testHiveMonadCustomReducers
  can hive monad conf returns the default value if jobConf reducer value matches MR reducer value   $testHiveMonadDefaultReducers
  can hive monad conf returns the correct values for speculative tasks and job cleanup              $testHiveSpecualtiveAndCleanupConf

"""

  val reducers = "1024"
  val reducerFromMRSite = "3"
  val config = new JobConf(true)

  def testGetHiveConfigCustomReducers = {

    val exec = for {
      conf         <- Execution.getConfig
      hc            = getHiveConf(conf)
      obtained      = hc.get(reducerConfig)
      _            <- Execution.guard(obtained == reducers, s"obtained reducer value: ${obtained}, expected: ${reducers}")
    } yield JobFinished
    jobConf.set(reducerConfig, reducers)
    executesSuccessfully(exec) must_== JobFinished
  }

  def testGetHiveConfDefaultReducers = {
    val exec = for {
      conf         <- Execution.getConfig
      hc            = getHiveConf(conf)
      obtained      = hc.get(reducerConfig)
      configured    = config.get(reducerConfig)
      _            <- Execution.guard(obtained == defaultHiveReducers, s"obtained reducer value: ${obtained}, expected: ${defaultHiveReducers}")
      _            <- Execution.guard(configured == reducerFromMRSite, s"obtained reducer value: ${configured}, expected: ${reducerFromMRSite}")
    } yield JobFinished
    executesSuccessfully(exec) must_== JobFinished
  }

  def testHiveMonadCustomReducers = {
    val exec = for {
      hiveConf     <- Execution.hive(Hive.getConf)
      obtained      = hiveConf.get(reducerConfig)
      _            <- Execution.guard(obtained == reducers, s"obtained reducer value: ${obtained}, expected: ${reducers}")
    } yield JobFinished
    jobConf.set(reducerConfig, reducers)
    executesSuccessfully(exec) must_== JobFinished
  }

  def testHiveMonadDefaultReducers = {
    val exec = for {
      hiveConf     <- Execution.fromHive(Hive.getConf)
      obtained      = hiveConf.get(reducerConfig)
      _            <- Execution.guard(obtained == defaultHiveReducers, s"obtained reducer value: ${obtained}, expected: ${defaultHiveReducers}")
    } yield JobFinished
    executesSuccessfully(exec) must_== JobFinished
  }

  def testHiveSpecualtiveAndCleanupConf = {
    val exec = for {
      hiveConf            <- Execution.fromHive(Hive.getConf)
      obtainedClean        = hiveConf.get(mrCleanup)
      obtainedSpeculative  = hiveConf.get(mrSpeculative)
      _                   <- Execution.guard(obtainedClean == "false", s"obtained job cleanup value: ${obtainedClean}, expected: false")
      _                   <- Execution.guard(obtainedSpeculative == "true", s"obtained speculative task value: ${obtainedSpeculative}, expected: true")
    } yield JobFinished
    executesSuccessfully(exec) must_== JobFinished
  }

}
