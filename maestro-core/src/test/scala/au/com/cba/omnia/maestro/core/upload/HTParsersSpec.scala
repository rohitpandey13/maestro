package au.com.cba.omnia.maestro.core.upload

import au.com.cba.omnia.thermometer.core.ThermometerSpec

import au.com.cba.omnia.omnitool.{Error, Result}

class ParserHTSpec extends ThermometerSpec {
  def is = s2"""
Header Trailer Extracting properties
===============

  split the header and return Header case class   $splitHeader
  split the trailer and return Trailer case class $splitTrailer
  split the header with more number of element    $splitHeaderLong
  split the trailer with more number of element   $splitTrailerLong
  split the header with wrong type format         $splitWrongHeaderFormat
  split the trailer with wrong type format        $splitWrongTrailerFormat
"""

  def splitHeader =
    HeaderParsers.default("H|20150311|20150318131001|hls_20150301.dat") must_== Result.ok(Header("20150311","20150318131001","hls_20150301.dat"))

  def splitTrailer =
    TrailerParsers.default("T|12|23|acc") must_== Result.ok(Trailer(12, "23", "acc"))

  def splitHeaderLong =
    HeaderParsers.default("H|20150311|20150318131001|hls_20150301.dat|1234|675") must beLike { case Error(_) => ok }

  def splitTrailerLong =
    TrailerParsers.default("T|12|23|acc|45") must beLike { case Error(_) => ok }

  def splitWrongHeaderFormat =
    HeaderParsers.default("D|20150311|20150318131001|hls_20150301.dat") must beLike { case Error(_) => ok }

  def splitWrongTrailerFormat =
    TrailerParsers.default("D|12|23|acc") must beLike { case Error(_) => ok }
}
