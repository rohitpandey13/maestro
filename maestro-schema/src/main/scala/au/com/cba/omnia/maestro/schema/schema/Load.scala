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

import scala.io.Source

import scala.util.parsing.combinator.Parsers
import scala.util.parsing.input.{NoPosition, Position, Reader}
import scala.util.parsing.json.{JSON}

import au.com.cba.omnia.maestro.schema.SchemaParser._


object Load {

  /** Type template for the JSON schema file. */
  type JsonSchema
    = Map[String,                 // "columns"
          List[Map[String, _]]]   // Meta-data about each column.


  /** Load column names and formats from the JSON schema file at the given path. */
  def loadFormats(fileName: String): List[(String, Option[Format])] = {

    // Read the taste file, forcing the last line to be \n terminated.
    val strSchema: String =
      Source.fromFile(fileName)
        .getLines .mkString ("\n")

    // Do initial JSON parsing of schema file.
    val jsonSchema: JsonSchema =
      JSON.parseFull(strSchema)
          .getOrElse { throw new Exception("Cannot parse schema file as JSON.") }
          .asInstanceOf[JsonSchema]

    // Get names from the taste file.
    val names: List[String] =
      (for {
        cols  <- jsonSchema.get("columns")
        name  <- sequence(cols.map { col =>
            for { nm <- col.get("name") }
            yield nm.asInstanceOf[String] })
      } yield name)
      .getOrElse (throw new Exception("Cannot parse names from JSON schema."))

    // Get format strings the taste file.
    val rawFormats: List[String] =
      (for {
        cols  <- jsonSchema.get("columns")
        name  <- sequence(cols.map { col =>
            for { nm <- col.get("format") }
            yield nm.asInstanceOf[String] })
      } yield name)
      .getOrElse (throw new Exception("Cannot parse formats from JSON schema."))

    def parseFormat(s: String): Option[Format]
      = parseString(pOptFormat, s) match {
          case Left(_)  => throw new Exception("Cannot parse format string.")
          case Right(f) => f
        }

    val formats: List[Option[Format]] =
      rawFormats
        .map { s => parseFormat(s) }

    // The names and format values are both extracted from the columns field
    // of the JSON schema file, so have the same length.
    names .zip (formats)
  }


  /** ZipWith-style helper. */
  def map2[A,B,C](a: Option[A], b: Option[B])(f: (A,B) => C): Option[C] =
    a.flatMap { x => b.map { y => f(x, y) } }


  /** Missing Haskell-esque sequence function. */
  def sequence[A](a: List[Option[A]]): Option[List[A]] =
    a.foldRight[Option[List[A]]](Some(Nil))((x,y) => map2(x,y)(_ :: _))
}
