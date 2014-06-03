package au.com.cba.omnia.maestro.core
package task

import java.nio.ByteBuffer
import java.security.{MessageDigest, SecureRandom}

import scala.collection.JavaConverters._
import scala.util.hashing.MurmurHash3

import scalaz.{Tag => _, _}, Scalaz._

import com.google.common.base.Splitter

import cascading.flow.{FlowDef, FlowProcess}
import cascading.operation._
import cascading.tuple.Tuple

import com.twitter.scalding._, TDsl._, Dsl._

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.maestro.core.codec._
import au.com.cba.omnia.maestro.core.clean.Clean
import au.com.cba.omnia.maestro.core.validate.Validator
import au.com.cba.omnia.maestro.core.filter.RowFilter
import au.com.cba.omnia.maestro.core.scalding.Errors

sealed trait TimeSource {
  def getTime(path: String): String = this match {
    case Predetermined(time) => time
    case FromPath(extract)  => extract(path)
  }
}

case class Predetermined(time: String) extends TimeSource
case class FromPath(extract: String => String) extends TimeSource

object Load extends Load

trait Load {
  /**
    * Loads the supplied text files and converts them to the specified thrift struct.

    * The operations performed are:
    *  1. Append a time field to each line using the provided time source.
    *  1. Split each line into columns/fields using the provided delimiter.
    *  1. Apply the provided filter to each list of fields.
    *  1. Clean each field using the provided cleaner.
    *  1. Convert the list of fields into the provided thrift struct.
    *  1. Validate each struct.
    */
  def load[A <: ThriftStruct : Decode : Tag : Manifest]
    (delimiter: String, sources: List[String], errors: String, timeSource: TimeSource, clean: Clean,
      validator: Validator[A], filter: RowFilter)
    (implicit flowDef: FlowDef, mode: Mode): TypedPipe[A] = {
    loadProcess(
      sources.map(p => TextLine(p).map(l => s"$l$delimiter${timeSource.getTime(p)}")).reduceLeft(_ ++ _),
      delimiter,
      errors,
      clean,
      validator,
      filter
    )
  }

  /**
    * Same as `load` but also appends a unique key to each line. The last field of `A`
    * needs to be set aside to receive the key as type string.
    * 
    * The key is generated by:
    *  1. Creating a random 4 byte seed for each load
    *  1. For each file hashing the seed and the path and then taking the last 8 bytes.
    *  1. Hashing each line and taking 12 bytes, taking the map task number (current slice number) and the offset into the file.
    *  1. Concatenating the file hash from 2, the slice number, byte offset and line hash
    * 
    * This produces a 256 bit key.
    */
  def loadWithKey[A <: ThriftStruct : Decode : Tag : Manifest]
    (delimiter: String, sources: List[String], errors: String, timeSource: TimeSource, clean: Clean,
      validator: Validator[A], filter: RowFilter)
    (implicit flowDef: FlowDef, mode: Mode): TypedPipe[A] = {
    val d    = delimiter
    val rnd  = new SecureRandom()
    val seed = rnd.generateSeed(4)
    val md = MessageDigest.getInstance("SHA-1")
    val hashes =
      sources
        .map(k => k -> md.digest(seed ++ k.getBytes("UTF-8")).drop(12).map("%02x".format(_)).mkString)
        .toMap

    loadProcess(
      sources.map(p =>
        TextLine(p)
          .read
          .each(('offset, 'line), 'result)(_ => new GenerateKey(delimiter, p, timeSource, hashes))
          .toTypedPipe[String]('result)
      ).reduceLeft(_ ++ _),
      delimiter,
      errors,
      clean,
      validator,
      filter
    )
  }

  def loadProcess[A <: ThriftStruct : Decode : Tag : Manifest]
    (in: TypedPipe[String], delimiter: String, errors: String, clean: Clean,
      validator: Validator[A], filter: RowFilter)
    (implicit flowDef: FlowDef, mode: Mode): TypedPipe[A] =
    Errors.safely(errors) {
      in
        .map(line => Splitter.on(delimiter).split(line).asScala.toList)
        .flatMap(filter.run(_).toList)
        .map(record =>
          Tag.tag[A](record).map { case (column, field) => clean.run(field, column) }
        ).map(record => Decode.decode[A](UnknownDecodeSource(record)))
        .map {
          case DecodeOk(value) =>
            validator.run(value).disjunction.leftMap(errors => s"""The following errors occured: ${errors.toList.mkString(",")}""")
          case e @ DecodeError(remainder, counter, reason) =>
            reason match {
              case ValTypeMismatch(value, expected) =>
                s"unexpected type: $e".left
              case ParseError(value, expected, error) =>
                s"unexpected type: $e".left
              case NotEnoughInput(required, expected) =>
                s"not enough fields in record: $e".left
              case TooMuchInput =>
                s"too many fields in record: $e".left
            }
      }
    }
}

class GenerateKey(delimiter: String, path: String, timeSource: TimeSource, hashes: Map[String, String])
    extends BaseOperation[(ByteBuffer, MessageDigest, Tuple)](('result)) with Function[(ByteBuffer, MessageDigest, Tuple)] {
  val d = delimiter

  def uid(path: String, slice: Int, offset: Long, line: String, byteBuffer: ByteBuffer, md: MessageDigest): String = {
    val hash = hashes(path)
    val lineHash = md.digest(line.getBytes("UTF-8")).drop(8)

    byteBuffer.clear

    val lineInfo  = byteBuffer.putInt(slice).putLong(offset).array

    val hex = (lineInfo ++ lineHash).map("%02x".format(_)).mkString

    s"$hash$hex"
  }

  override def prepare(flow: FlowProcess[_], call: OperationCall[(ByteBuffer, MessageDigest, Tuple)]): Unit = {
    val md = MessageDigest.getInstance("SHA-1")
    call.setContext((ByteBuffer.allocate(12), md, Tuple.size(1)));
  }

  def operate(flow: FlowProcess[_], call: FunctionCall[(ByteBuffer, MessageDigest, Tuple)]): Unit = {
    val entry  = call.getArguments
    val offset = entry.getLong(0)
    val line   = entry.getString(1)
    val slice  = flow.getCurrentSliceNum

    val (byteBuffer, md, resultTuple) = call.getContext
    val keyedLine = s"$line$d${timeSource.getTime(path)}$d${uid(path, slice, offset, line, byteBuffer, md)}"

    resultTuple.set(0, keyedLine)
    call.getOutputCollector.add(resultTuple)
  }
}