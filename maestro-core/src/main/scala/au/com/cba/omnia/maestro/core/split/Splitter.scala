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

package au.com.cba.omnia.maestro.core
package split

import scala.collection.JavaConverters._

import com.google.common.base.{Splitter => GoogSplitter}

import com.opencsv.CSVParser

/**
  * Represents the notion of splitting a string into parts
  *
  * There is no requirement that `splitter.run(s).mkString == s`.
  * However, it should be theoretically possible to write a function
  * `List[String] => String` that reverses the split on good input.
  *
  * Originally the idea was that Splitter would return `String \/ List[String]`.
  * However, the code I wrote in [[au.com.cba.omnia.maestro.core.task.Load]]
  * to handle this caused scalding serialization to stop working, so now the
  * splitters try to do "sensible" things if they see something incorrect, where
  * "sensible" means return a result that will cause a sensible error later.
  */
case class Splitter(run: String => List[String])

/** Factory for [[au.com.cba.omnia.maestro.core.split.Splitter]] instances.  */
object Splitter {

  /**
    * Creates a splitter that splits delimited strings.
    *
    * Valid for all input.
    */
  def delimited(delimiter: String) =
    Splitter(s => GoogSplitter.on(delimiter).split(s).asScala.toList)

  /**
    * Creates a splitter that uses a CSV parser, and hence can handle quoted strings.
    *
    * Valid for all input which doesn't end in the middle of a quoted string.
    */
  def csv(delimiter: Char) =
    Splitter(CsvParseFunction(delimiter))

  private case class CsvParseFunction(delimiter: Char) extends Function[String, List[String]] {
    // parser will not be created until function is used, and so does not have to be serialized
    lazy val parser = new CSVParser(delimiter)
    def apply(line: String): List[String] =
      parser.parseLine(line).toList
  }

  /**
    * Creates a splitter that splits according to predefined column lengths.
    *
    * Valid on strings containing exactly the number of characters required to
    * fill all columns.
    *
    * When passed invalid input (wrong number of characters), we ensure that
    * `Splitter.fixed(lengths).run(invalid).length != lengths.length`.
    */
  def fixed(lengths: List[Int]) = {
    val starts = lengths.scanLeft(0)(_ + _)
    val ends = starts.tail
    val totalLength = starts.last
    Splitter(s =>
      // we try to be "sensible" by ensuring that, if s has incorrect length,
      // the number of values we return != lengths.length

      if (s.length > totalLength) {
        // if we have extra characters, add an extra column to contain them
        // so we return lengths.length + 1 values, and
        // hopefully the parsing framework will throw an error on this later
        (starts, ends).zipped.map(s.substring(_,_)) ++ List(s.substring(totalLength, s.length))
      }
      else if (s.length < totalLength) {
        // if we have less characters, we only return fields we have complete text for
        // thus ensuring we return less values then lengths.length
        // hopefully the parsing framework will throw an error on this later
        val endsPresent = ends.takeWhile(_ <= s.length)
        (starts, endsPresent).zipped.map(s.substring(_,_))
      }
      else {
        (starts, ends).zipped.map(s.substring(_,_))
      }
    )
  }
}
