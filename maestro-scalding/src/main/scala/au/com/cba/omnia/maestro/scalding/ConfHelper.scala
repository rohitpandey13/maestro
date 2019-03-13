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

import org.slf4j.LoggerFactory

import com.twitter.scalding.{Config, UniqueID}

/** Collection of utility methods around get/setting Config. */
object ConfHelper {

  val logger = LoggerFactory.getLogger(this.getClass)
  val reducerConfig = "mapreduce.job.reduces"
  val mrSpeculative = "mapreduce.reduce.speculative"
  val mrCleanup = "mapreduce.job.committer.setup.cleanup.needed"
  val defaultHiveReducers = "-1"

  /** Get the Hadoop JobConfig from Config. */
  def getHadoopConf(config: Config): JobConf = {
    val conf = new JobConf(true)
    config.toMap.foreach{ case (k, v) => conf.set(k, v) }
    conf
  }

  /** Returns the reducer value based on scalding job config value.
    * If the reducer value obtained from scalding job is different than MR cluster config, the job config value will be used.
    * Else -1 will be set as reducer value, so the dynamic reducer will be configured for Hive */
  def getReducers(config: Config) = {
    val conf = new JobConf(true)
    val defaultMRReducers = conf.get(reducerConfig, defaultHiveReducers)
    val jobConfMRReducers = config.get(reducerConfig).getOrElse(defaultHiveReducers)
    if (defaultMRReducers == jobConfMRReducers) {
      logger.warn(s"job configured reducers is same as default MR reducers ${jobConfMRReducers}, for hiveconf the reducer will be set as: ${defaultHiveReducers}")
      defaultHiveReducers
    }
    else {
      logger.info(s"job config override the number of reducers as ${jobConfMRReducers}, which will be used for the job")
      jobConfMRReducers
    }
  }

  /** Used for Hive Config, by configuring reducer based on the job config and cluster config
    * and override the default values for `mapreduce.reduce.speculative` and `mapreduce.job.committer.setup.cleanup.needed` based on Hive defaults
    * TODO: This is hack for fixing dynamic reducer issue, this needs to be revisited when we migrate to beeline */
  def getHiveConf(config: Config): JobConf = {
    val conf = getHadoopConf(config)
    conf.set(reducerConfig, getReducers(config))
    conf.setBoolean(mrSpeculative, true)
    conf.setBoolean(mrCleanup, false)
    conf
  }

  /**
    * Get cascading to create unique output filenames by inserting a timestamp as part of the output
    * filename.
    *
    * This is used to get cascading to append to files to existing partitions.
    */
  def createUniqueFilenames(config: Config): Config = {
    config + ("cascading.tapcollector.partname" -> s"%s%spart-${UniqueID.getRandom.get}-%05d-%05d")
  }
}
