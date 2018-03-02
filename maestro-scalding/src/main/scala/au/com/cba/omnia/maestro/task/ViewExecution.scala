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

package au.com.cba.omnia.maestro.task

import com.twitter.scalding.{Execution, TupleSetter, TypedPipe, Stat}

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.ebenezer.scrooge.PartitionParquetScroogeSink
import au.com.cba.omnia.beeswax.Hive

import au.com.cba.omnia.maestro.core.partition.Partition
import au.com.cba.omnia.maestro.hive.HiveTable
import au.com.cba.omnia.maestro.scalding.StatKeys
import au.com.cba.omnia.maestro.scalding.ExecutionOps._
/**
  * Configuration options for write pipes to HDFS files.
  *
  * @param partition: The partition used to split pipe data.
  * @param output:    The HDFS file in which to store data.
  */
case class ViewConfig[A <: ThriftStruct, B](
  partition: Partition[A, B],
  output: String
)

/** Executions for view tasks */
trait ViewExecution {
  /**
    * Partitions a pipe using the given partition scheme and writes out the data.
    *
    * @return the number of rows written.
    */
  def view[A <: ThriftStruct : Manifest, B : Manifest : TupleSetter](
    config: ViewConfig[A, B], pipe: TypedPipe[A]
  ): Execution[Long] =
    Execution.withId { id =>
      val statKey = StatKeys.tuplesOutput
      val stat    = Stat(statKey)(id)

      pipe
        .map { v => stat.inc; config.partition.extract(v) -> v }
        .writeExecution(PartitionParquetScroogeSink[B, A](config.partition.pattern, config.output))
        .getAndResetCounters
        .map { case (_, counters) => counters.get(statKey).getOrElse(0) }
    }

  /**
    * Writes out the data to a hive table.
    *
    * This will create the table if it doesn't already exist. If the existing schema doesn't match
    * the schema expected the job will fail.
    *
    * @return the number of rows written.
    */
  def viewHive[A <: ThriftStruct : Manifest, ST](
    table: HiveTable[A, ST], pipe: TypedPipe[A], append: Boolean = true
  ): Execution[Long] = Execution.withId { id => for {
    /* Creates the database upfront since when Hive is run concurrently uzing `zip` all but the
     * first attempt fails.
     * The Hive monad handles this so that the job doesn't fall over.
     */
    _      <- Execution.fromHive(Hive.createDatabase(table.database))
    statKey = StatKeys.tuplesOutput
    stat    = Stat(statKey)(id)
    n      <- table.writeExecution(pipe.map(row => { stat.inc; row }), append).map(_.get(statKey).getOrElse(0L))
  } yield n }
}
