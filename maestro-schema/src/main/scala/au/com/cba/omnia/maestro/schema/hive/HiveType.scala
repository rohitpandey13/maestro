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
package hive


/** The storage types that Hive supports. */
object HiveType extends Enumeration {
  type HiveType = Value

  val  HiveString, HiveDouble, HiveInt, HiveBigInt, HiveSmallInt = Value


  /** Maps Hive types to the names used by Hive. */
  val names: Map[HiveType, String] = Map(
    HiveString    -> "string",
    HiveDouble    -> "double",
    HiveInt       -> "int",
    HiveBigInt    -> "bigint",
    HiveSmallInt  -> "smallint")


  /** Parse a string as a Hive type, using the Hive names. */
  def parse(s: String): Option[HiveType] = 
    names 
      .find { case (t, n) => n == s }
      .map  { case (t, n) => t }


  /** Pretty print a Hive type, using the Hive names. */
  def pretty(h: HiveType): String = 
    names(h)
}

