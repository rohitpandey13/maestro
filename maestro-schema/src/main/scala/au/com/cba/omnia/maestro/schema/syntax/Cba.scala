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
package syntax

import  au.com.cba.omnia.maestro.schema._


/** Three letter CBA financial system codes, like "SAP". */
object CbaSystemCode extends Syntax {

  val name = "CbaSystemCode"

  def likeness(s: String): Double = 
    if (codes(s)) 1.0 else 0.0

  val codes = Set(
          "AFS"
        , "BCB", "BPS", "BQM"
        , "CAB", "CBD", "CBS", "CCS", "CIL", "CIF", "CLS", "CMP", "CSE", "CSL"
        , "CMS", "CHR"
        , "DDS", "DMM"
        , "FAC", "FMS"
        , "HLS"
        , "LIF", "LMS"
        , "MAS", "MID", "MLN", "MLS", "MNY", "MRX", "MSG", "MTX"
        , "PAX", "PLS"
        , "RVR"
        , "SAP", "SAV", "SIN"
        , "TDS", "THA", "TFS"
        , "WSS"
        , "YCS")

  val parents:    Set[Syntax] = Set(Upper)
  val partitions: Set[Syntax] = Set()
}


/** CBA Account codes, which start with a three letter system code.
 *  We assume anything with a system code followed by at least three
 *  five more charaters is an account code. */
object CbaAccountCode extends Syntax {
  
  val name = "CbaAccountCode"

  def likeness(s: String): Double = 
    if      (s.length < 3) 
         0.0
    else if (CbaSystemCode.codes(s.substring(0, 3)) 
             && s.length >= 8)
         1.0
    else 0.0

  val parents:    Set[Syntax] = Set(Any)
  val partitions: Set[Syntax] = Set()
}

