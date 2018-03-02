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

package au.com.cba.omnia.maestro.schema
package taste

import scala.io.Source

import scala.util.parsing.combinator.Parsers
import scala.util.parsing.input.{NoPosition, Position, Reader}
import scala.util.parsing.json.{JSON}

import au.com.cba.omnia.maestro.schema._

object Load {

  /** Type template for the JSON taste file. */
  type JsonTaste
    = Map[String,                 // "columns"
          List[Map[String, _]]]   // Meta-data about each column.


  /** Load histograms from JSON taste file at the given path.
   *  Also load names from a names file if we have one. */
  def loadNamesTaste(
      fileNames: Option[String],
      fileTaste: String)
    : (List[(String, Histogram)]) = {

    // Read the taste file, forcing the last line to be terminated by \n.
    val strTaste: String =
      Source.fromFile(fileTaste)
        .getLines .mkString ("\n")

    // Do initial JSON parsing of taste file.
    val jsonTaste: JsonTaste =
      JSON.parseFull(strTaste)
          .getOrElse { throw new Exception("Cannot parse taste file as JSON.") }
          .asInstanceOf[JsonTaste]

    // Get names from the taste file, if there are any.
    val namesTaste: Option[List[String]] =
      (for {
        rows  <- jsonTaste.get("columns")
        name  <- sequence(rows.map { row =>
            for { nm <- row.get("name") }
            yield nm.asInstanceOf[String] })
      } yield name)

    // Get names from the external names file, if we have one.
    val namesExt: Option[List[String]] =
      fileNames.map { file =>
        sequence(Source.fromFile(file)
          .getLines
          .map       { w  => w  .split("\\s+").toList }
          .map       { ws => ws .lift (0) }
          .toList)
        .getOrElse { throw new Exception("Cannot parse names file.") }
        }

    // Decide where the names come from.
    val names: List[String] =
      namesExt match {
        case Some(names)   => names
        case None          => namesTaste match {
          case Some(names) => names
          case None        =>
            throw new Exception(
              "No column names in taste file.\n" +
              "and an external names file was not provided.")
          }
        }

    // Get a list of classifier names and counts for each column
    // from the taste file.
    val histsRaw: List[List[(String, Int)]] =
      (for {
        rows  <- jsonTaste.get("columns")
        clas  <- sequence(rows.map { row =>
          for { cl <- row.get("classifiers") }
          yield cl.asInstanceOf[Map[String,_]] })
       } yield clas)
      .getOrElse { throw new Exception("Cannot parse classifiers. ") }

      // Truncate incoming classifier counts to integers.
      .map ( m => m.toList.map { case (s, d) =>
        (s, d.asInstanceOf[Double].toInt) })

    // Parse the classifier strings and produce a real histogram for each row.
    val hists: List[Histogram] =
      histsRaw.map { counts => parseHistogram(counts) }

    // The names and hists values are both extracted from the columns field
    // of the JSON schema file, so have the same length.
    names .zip (hists)
  }


  /** Load a Histogram from the raw JSON. */
  def parseHistogram(counts: List[(String, Int)]): Histogram = {
    val clas  =
      counts.map { case (s, d) =>
        SchemaParser.parseString(SchemaParser.pClassifier, s) match {
          case Left(err) => throw new Exception("Cannot parse classifier: " + s)
          case Right(c)  => (c, d)
        }
      }
    Histogram(clas.toMap)
  }


  /** ZipWith-style helper. */
  def map2[A,B,C](a: Option[A], b: Option[B])(f: (A,B) => C): Option[C] =
    a.flatMap { x => b.map { y => f(x, y) } }


  /** Missing Haskell-esque sequence function. */
  def sequence[A](a: List[Option[A]]): Option[List[A]] =
    a.foldRight[Option[List[A]]](Some(Nil))((x,y) => map2(x,y)(_ :: _))
}
