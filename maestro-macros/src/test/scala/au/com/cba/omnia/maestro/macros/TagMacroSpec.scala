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

import au.com.cba.omnia.omnitool.Result

import au.com.cba.omnia.maestro.test.Spec
import au.com.cba.omnia.maestro.test.thrift.humbug.{Types => HTypes}
import au.com.cba.omnia.maestro.test.thrift.scrooge.{Types => STypes}

object TagMacroSpec extends Spec { def is = s2"""

TagMacro
===========

The tag macro creates tag

  with the names of the thrift fields for humbug  $tagHumbug
  with the names of the thrift fields for scrooge $tagScrooge
"""

  def tagHumbug = {
    //given
    val typesList = List("stringField", "booleanField", "intField", "longField", "doubleField", "optIntField", "optStringField")

    //when
    val humbugTag  = Macros.mkTag[HTypes]

    //then
    val humbugFields = humbugTag.run(List("1", "2", "3", "4", "5", "6", "7"))
    humbugFields.map(_.map({ case (_, field) => field.name })) === Result.ok(typesList)
  }

  def tagScrooge = {
    //given
    val typesList = List("stringField", "booleanField", "intField", "longField", "doubleField", "optIntField", "optStringField")

    //when
    val scroogeTag = Macros.mkTag[STypes]

    //then
    val scroogeFields = scroogeTag.run(List("1", "2", "3", "4", "5", "6", "7"))
    scroogeFields.map(_.map({ case (_, field) => field.name })) === Result.ok(typesList)
  }
}
