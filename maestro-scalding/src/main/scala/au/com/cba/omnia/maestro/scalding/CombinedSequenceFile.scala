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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapred.lib._

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.scheme.hadoop.{WritableSequenceFile => CHWritableSequenceFile}
import cascading.tap.Tap
import cascading.tuple.Fields

import com.twitter.scalding._

/** Keys for job conf properties */
object ConfKeys {
  val combinedSeqFileMaxSplitSize = "maestro.combinedseqfile.max.split.size"
}

/** A WritableSequenceFile which merges the input splits sent to downstream mappers. */
class CombinedSequenceFile[K <: Writable : Manifest, V <: Writable : Manifest](
  val path: String, splitSize: Long
) extends WritableSequenceFile[K, V](path, new Fields(0, 1)) {

  override def hdfsScheme =
    (new CombinedSequenceFileScheme(fields, keyType, valueType, splitSize))
      .asInstanceOf[Scheme[JobConf,RecordReader[_,_],OutputCollector[_,_],_,_]]
}

/** Factory for CombinedSequenceFile */
object CombinedSequenceFile {
  def apply[K <: Writable : Manifest, V <: Writable : Manifest](path: String, splitSize: Long) =
    new CombinedSequenceFile[K, V](path, splitSize)
}

/**
  * The cascading scheme for CombinedSequenceFile
  *
  * We only merge the splits sent to downstream mappers, not the data coming
  * from upstream.
  */
class CombinedSequenceFileScheme(
  fields: Fields,
  keyType: Class[_ <: Writable],
  valueType: Class[_ <: Writable],
  splitSize: Long
) extends CHWritableSequenceFile(fields, keyType, valueType) {
  override def sourceConfInit(
    flowProcess: FlowProcess[JobConf],
    tap: Tap[JobConf, RecordReader[_,_], OutputCollector[_,_]],
    conf: JobConf
  ) {
    conf.setLong(ConfKeys.combinedSeqFileMaxSplitSize, splitSize)
    conf.setInputFormat(classOf[CombineSequenceFileInputFormat[_,_]])
  }
}

/**
  * The input format for CombinedSequenceFile
  *
  * When we upgrade to a later version of hadoop, we should be able to remove
  * this class in favor of hadoop's version.
  */
class CombineSequenceFileInputFormat[K,V]() extends CombineFileInputFormat[K,V] {

  override def getSplits(conf: JobConf, numSplits: Int): Array[InputSplit] = {
    val maxSplitSize = conf.getLong(ConfKeys.combinedSeqFileMaxSplitSize, -1L)
    if (maxSplitSize < 0L) { throw new Exception(s"Could not find configuration parameter ${ConfKeys.combinedSeqFileMaxSplitSize}")}
    setMaxSplitSize(maxSplitSize)

    super.getSplits(conf, numSplits)
  }

  override def getRecordReader(split: InputSplit, conf: JobConf, reporter: Reporter): RecordReader[K,V] = {
    val innerReader = classOf[SequenceFileRecordReaderWrapper[K,V]].asInstanceOf[Class[RecordReader[K,V]]]
    new CombineFileRecordReader(conf, split.asInstanceOf[CombineFileSplit], reporter, innerReader)
  }
}

/**
  * The reader that CombineFileRecordReader delegates to to read from the sequence file
  *
  * This is just a wrapper that converts the (CombineFileSplit, index) pair supplied
  * by CombineFileRecordReader into the FileSplit required by SequenceFileInputFormat
  */
class SequenceFileRecordReaderWrapper[K,V](split: CombineFileSplit, conf: Configuration, reporter: Reporter, idx: Integer) extends RecordReader[K,V] {
  val fileSplit = new FileSplit(
    split.getPath(idx),
    split.getOffset(idx),
    split.getLength(idx),
    split.getLocations()
  )
  val jobConf = conf.asInstanceOf[JobConf]
  val inner   = new SequenceFileInputFormat[K,V]().getRecordReader(fileSplit, jobConf, reporter)

  def next(key: K, value: V): Boolean = inner.next(key, value)
  def close { inner.close }

  def createKey:   K     = inner.createKey
  def createValue: V     = inner.createValue
  def getPos:      Long  = inner.getPos
  def getProgress: Float = inner.getProgress
}
