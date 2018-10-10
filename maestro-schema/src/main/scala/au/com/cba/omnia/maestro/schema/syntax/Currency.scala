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


/** Three letter curreny codes, like "AUD", "USD", "CHF". */
object CurrencyCode extends Syntax {
  
  val name = "CurrencyCode"

  def likeness(s: String): Double = 
    if (codesISO(s) || codesOther(s)) 1.0 else 0.0

  // ISO standard currency codes.
  val codesISO = Set(
      "AED", "AFN", "ALL", "AMD", "ANG", "AOA", "ARS", "AUD", "AWG", "AZN"
    , "BAM", "BBD", "BDT", "BGN", "BHD", "BIF", "BMD", "BND", "BOB", "BRL"
    , "BSD", "BTN", "BWP", "BYR", "BZD"
    , "CAD", "CDF", "CHF", "CLP", "CNY", "COP", "CRC", "CUC", "CUP", "CVE"
    , "CZK"
    , "DJF", "DKK", "DOP", "DZD"
    , "EGP", "ERN", "ETB", "EUR"
    , "FJD", "FKP"
    , "GBP", "GEL", "GGP", "GHS", "GIP", "GMD", "GNF", "GTQ", "GYD"
    , "HKD", "HNL", "HRK", "HTG", "HUF"
    , "IDR", "ILS", "IMP", "INR", "IQD", "IRR", "ISK"
    , "JEP", "JMD", "JOD", "JPY"
    , "KES", "KGS", "KHR", "KMF", "KPW", "KRW", "KWD", "KYD", "KZT"
    , "LAK", "LBP", "LKR", "LRD", "LSL", "LTL", "LVL", "LYD"
    , "MAD", "MDL", "MGA", "MKD", "MMK", "MNT", "MOP", "MRO", "MUR", "MVR"
           , "MWK", "MXN", "MYR", "MZN"
    , "NAD", "NGN", "NIO", "NOK", "NPR", "NZD"
    , "OMR"
    , "PAB", "PEN", "PGK", "PHP", "PKR", "PLN", "PYG"
    , "QAR"
    , "RON", "RSD", "RUB", "RWF"
    , "SAR", "SBD", "SCR", "SDG", "SEK", "SGD", "SHP", "SLL", "SOS", "SPL"
           , "SRD", "STD", "SVC", "SYP", "SZL"
    , "THB", "TJS", "TMT", "TND", "TOP", "TRY", "TTD", "TVD", "TWD", "TZS"
    , "UAH", "UGX", "USD", "UYU", "UZS"
    , "VEF", "VND", "VUV"
    , "WST"
    , "XAF", "XCD", "XDR", "XOF", "XPF"
    , "YER"
    , "ZAR", "ZMW", "ZWD")

  // Non-ISO currency codes.
  // Includes pre-euro currencies, and bullion codes.
  val codesOther = Set(
      "CNH", "CNT", "TRL", "FIM", "SKK", "ESP", "ZWR", "MTP", "VEB"
    , "CYP", "GRD", "MTL"
    , "FRF", "DEM", "ITL", "YUN", "ATS", "BEF", "PTE", "NLG", "IEP"
    , "XAU", "XAG", "XPT", "XPD", "XEU")

  val parents:    Set[Syntax] = Set(Upper)
  val partitions: Set[Syntax] = Set()
}

