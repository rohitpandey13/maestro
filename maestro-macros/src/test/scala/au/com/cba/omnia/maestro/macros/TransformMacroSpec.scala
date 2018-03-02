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

import au.com.cba.omnia.maestro.test.Spec
import au.com.cba.omnia.maestro.test.Arbitraries._
import au.com.cba.omnia.maestro.test.thrift.humbug.{Types => HTypes, Large, SubOne, SubTwo, SubThree}
import au.com.cba.omnia.maestro.test.thrift.scrooge.{Types => STypes}

object TransformMacroSpec extends Spec { def is = s2"""
TransformMacroSpec
==================

For Humbug
  Can transform one struct to another without manual operations $autoHumbug
  Can transform one struct to another with manual operations $manualHumbug

For Scrooge
  Can transform one struct to another without manual operations $autoScrooge
  Can transform one struct to another with manual operations $manualScrooge
"""

  implicit val srcFields = Macros.mkFields[HTypes]
  implicit val dstFields = Macros.mkFields[Large]

  def autoHumbug = prop { (types: HTypes) =>
    val s1 = new SubOne()
    s1.stringField  = types.stringField
    s1.booleanField = types.booleanField

    val s2 = new SubTwo()
    s2.intField    = types.intField
    s2.longField   = types.longField
    s2.doubleField = types.doubleField

    val s3 = new SubThree()
    s3.longField = types.longField
    s3.intField  = types.intField

    Macros.mkTransform[HTypes, SubOne]().run(types)   must_== s1
    Macros.mkTransform[HTypes, SubTwo]().run(types)   must_== s2
    Macros.mkTransform[HTypes, SubThree]().run(types) must_== s3
  }

  def manualHumbug = prop { (types: HTypes) =>
    val s1 = new SubOne()
    s1.stringField  = types.stringField
    s1.booleanField = !types.booleanField

    val s2 = new SubTwo()
    s2.intField    = types.intField + 1
    s2.longField   = types.longField - 1
    s2.doubleField = types.doubleField

    val t1 = Macros.mkTransform[HTypes, SubOne](
      ('booleanField, (x: HTypes) => !x.booleanField)
    )

    val t2 = Macros.mkTransform[HTypes, SubTwo](
      ('intField,  (x: HTypes) => x.intField + 1),
      ('longField, (x: HTypes) => x.longField - 1)
    )

    t1.run(types) must_== s1
    t2.run(types) must_== s2
  }

  def autoScrooge = prop { (types: STypes) =>
    Macros.mkTransform[STypes, STypes]().run(types) must_== types
  }

  def manualScrooge = prop { (types: STypes) =>
    val expected = types.copy(
      stringField = types.stringField + "1",
      optIntField = types.optIntField.map(_ - 1)
    )

    val t = Macros.mkTransform[STypes, STypes](
      ('stringField, (x: STypes) => x.stringField  + "1"),
      ('optIntField, (x: STypes) => x.optIntField.map(_ - 1))
    )

    t.run(types) must_== expected
  }

  // Test cases for errors. These should not compile
  /*def missingField = {
    Macros.mkTransform[SubThree, SubTwo]()
  }

  def invalidDstType = {
    Macros.mkTransform[SubThree, SubTwo](('doubleField, (t: SubThree) => t.intField))
  }

  def invalidDstTypeDefault = {
    Macros.mkTransform[SubOne, WrongType]()
  }

  def invalidSymbol = {
    val x = 'doubleField
    Macros.mkTransform[SubThree, SubTwo]((x, (t: SubThree) => t.intField.toDouble))
  }

  def tooMany = {
    Macros.mkTransform[SubThree, SubTwo](
      ('doubleField, (t: SubThree) => t.intField.toDouble),
      ('doubleField, (t: SubThree) => t.intField.toDouble)
    )
  }*/
}
