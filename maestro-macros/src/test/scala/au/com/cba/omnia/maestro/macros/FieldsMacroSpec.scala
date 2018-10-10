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

package au.com.cba.omnia.maestro.macros

import MacroSupport.Fields

import au.com.cba.omnia.maestro.test.Spec
import au.com.cba.omnia.maestro.test.Arbitraries._
import au.com.cba.omnia.maestro.test.thrift.humbug.{Types => HTypes}
import au.com.cba.omnia.maestro.test.thrift.scrooge.{Types => STypes}

object FieldsMacroSpec extends Spec { def is = s2"""

FieldsMacro
===========

The Fields macro creates fields

  with the name of the thrift field for humbug  $nameHumbug
  with the name of the thrift field for scrooge $nameScrooge
  that can extract a value for humbug           $extractHumbug
  that can extract a value for scrooge          $extractScrooge
  that satisfies an equality test               $satisfiesEquality

The DerivedFields macro supplies an implicit typeclass instance that provides

  the id-ordered list of fields for humbug      $implicitHumbug
  the id-ordered list of fields for scrooge     $implicitScrooge
"""

  val humbugFields  = Macros.mkFields[HTypes]
  val scroogeFields = Macros.mkFields[STypes]

  def nameHumbug = {
    humbugFields.StringField.name === "stringField"
    humbugFields.LongField.name   === "longField"
    humbugFields.DoubleField.name === "doubleField"
  }

  def nameScrooge = {
    scroogeFields.StringField.name === "stringField"
    scroogeFields.LongField.name   === "longField"
    scroogeFields.DoubleField.name === "doubleField"
  }

  def extractHumbug = prop { (t: HTypes) =>
    humbugFields.StringField.get(t) === t.stringField
    humbugFields.LongField.get(t)   === t.longField
    humbugFields.DoubleField.get(t) === t.doubleField
  }

  def extractScrooge = prop { (t: STypes) =>
    scroogeFields.StringField.get(t) === t.stringField
    scroogeFields.LongField.get(t)   === t.longField
    scroogeFields.DoubleField.get(t) === t.doubleField
  }

  def satisfiesEquality = {
    val fields = humbugFields
    val fieldList = fields.AllFields
    val stringField = fields.StringField

    fieldList must have size 7
    fieldList.contains(stringField) === true
  }

  // Example of a function (used in subsequent tests) requiring implicit evidence of Fields[T]
  def genericFieldNames[T : Fields] = implicitly[Fields[T]].AllFields.map(_.name)

  val fieldNamesOrderedByFieldId = List("stringField", "booleanField", "intField", "longField", "doubleField", "optIntField", "optStringField")

  def implicitHumbug = {
    genericFieldNames[HTypes] mustEqual fieldNamesOrderedByFieldId
  }

  def implicitScrooge = {
    genericFieldNames[STypes] mustEqual fieldNamesOrderedByFieldId
  }
}
