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

package au.com.cba.omnia.maestro.benchmark.split

import org.scalameter.api.{PerformanceTest, Gen}

import au.com.cba.omnia.maestro.core.split.Splitter

import au.com.cba.omnia.maestro.benchmark.thrift.Generators

object SplitterBenchmark extends PerformanceTest.OfflineReport {
  def testSplitter(gen: Gen[Array[List[String]]], combine: List[String] => String, splitter: Splitter) =
    using (gen.map(_.map(combine))) in { rows => {
      var i = 0
      while (i < rows.length) {
        splitter.run(rows(i))
        i = i+1
      }
    }}

  def sizes: Stream[Int] =
    Stream.continually(List(10, 10, 10, 6, 10)).flatten

  def mkFixed(row: List[String]) =
    (row zip sizes) map { case (field, size) => s"${size}s".format(field) } mkString

  performance of "Splitter" in {
    measure method "delimited[Struct10]" in testSplitter(Generators.struct10Rows, _.mkString("|"), Splitter.delimited("|"))
    measure method "delimited[Struct20]" in testSplitter(Generators.struct20Rows, _.mkString("|"), Splitter.delimited("|"))
    measure method "delimited[Struct30]" in testSplitter(Generators.struct30Rows, _.mkString("|"), Splitter.delimited("|"))

    measure method "csv[Struct10]" in testSplitter(Generators.struct10Rows, _.mkString("|"), Splitter.csv('|'))
    measure method "csv[Struct20]" in testSplitter(Generators.struct20Rows, _.mkString("|"), Splitter.csv('|'))
    measure method "csv[Struct30]" in testSplitter(Generators.struct30Rows, _.mkString("|"), Splitter.csv('|'))

    measure method "fixed[Struct10]" in testSplitter(Generators.struct10Rows, mkFixed, Splitter.fixed(sizes.take(10).toList))
    measure method "fixed[Struct20]" in testSplitter(Generators.struct20Rows, mkFixed, Splitter.fixed(sizes.take(20).toList))
    measure method "fixed[Struct30]" in testSplitter(Generators.struct30Rows, mkFixed, Splitter.fixed(sizes.take(30).toList))
  }
}
