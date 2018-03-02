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

package au.com.cba.omnia.maestro.api.test

import scalaz._, Scalaz._

import com.twitter.scalding.Execution

import org.specs2._

import au.com.cba.omnia.maestro.api._, Maestro._


/** 
  * Checks that the different syntax and implicit resolutions work as expected.
  * The fact that the code compiles is enough to ensure that the syntax works.
  * Hence this test won't actually run anything.
  */
object SyntaxSpec extends Specification { def is = s2"""

Maestro API Syntax Spec
=======================

 Scalaz syntax monad syntax works $bind

"""

  def bind = {
    Execution.from(3) >>= (a => Execution.from(a + 3))
    Execution.from(3) >> Execution.from(4)

    true
  }
}
