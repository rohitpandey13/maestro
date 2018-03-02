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

package au.com.cba.omnia.maestro.core.upload

import scala.util.control.NonFatal

import scala.util.parsing.combinator.Parsers
import scala.util.parsing.input.CharSequenceReader

import org.joda.time.{DateTime, DateTimeFieldType, MutableDateTime, DateTimeZone, Period}
import org.joda.time.format.DateTimeFormat

import scalaz._, Scalaz._

import au.com.cba.omnia.omnitool.time.DateFormatInfo
import au.com.cba.omnia.omnitool.Result

/** The results of a match against a file name */
sealed trait MatchResult

/** The file does not match the pattern */
case object NoMatch extends MatchResult

/**
  * The file matches the pattern.
  *
  * Also contains the directories for the file
  * corresponding to the date time fields in the pattern, the date time, and the inferred frequency.
  */
case class Match(dirs: List[String], stamp: DateTime, frequency: Period) extends MatchResult

/**
  * Contains parsers for input file patterns and input files
  *
  * The `forPattern` method is the most important one for users.
  * This method parses a file pattern and produces an appropriate
  * file name matcher.
  */
object InputParsers extends Parsers {

  /** convert an option to a Result - expected to be incorporated into omnitool. */
  def fromOption[A](option: Option[A], msg: String): Result[A] =
    option match {
      case Some(a) => Result.ok(a)
      case None    => Result.fail(msg)
  }

  /**
    * Pass in timezone, table name and file pattern and get back a matching function on file names.
    *
    * Both the pattern parsing and the matching functions can fail with an error message.
    */
  def forPattern(tz: DateTimeZone)(tableName: String, pattern: String): Result[String => Result[MatchResult]] =
    PatternParser.pattern(tableName, tz)(new CharSequenceReader(pattern)) match {
      case NoSuccess(msg, _) => Result.fail(msg)
      case Success(fileNameParser, _) => {
        val func: String => Result[MatchResult] =
          (fileName: String) => fileNameParser(new CharSequenceReader(fileName)) match {
            case Error(msg, _)    => Result.fail(msg)
            case Failure(_, _)    => Result.ok(NoMatch)
            case Success((dirs, stamp, frequency), _) => Result.ok(Match(dirs, stamp, frequency))
          }
        Result.ok(func)
      }
    }


  /** Throws parse error (not parse failure) if Result failed */
  def toParser[A](x : Result[A]): Parser[A] =
    x.foldMessage(a => success(a), msg => err(msg))

  type Elem = Char

  /**
    * File pattern parser
    *
    * Parses a file pattern and produces a `PartialParser`
    */
  type PatternParser = Parser[PartialParser]

  /**
    * Input file name parser
    *
    * Parses a file name and returns a list of the path parts to the destination
    * directory, (e.g., for daily files: YYYY/MM/DD) along with the
    * corresponding `DateTime` and inferred frequency (e.g., 1 day or 1 hour).
    * Unused components in the `DateTime` default to the least valid value
    * (E.g., for yearly files: January 1st 00:00).
    */
  type FileNameParser = Parser[(List[String], DateTime, Period)]

  /**
    * Building block of an input file name parser.
    *
    * Has a pre-determined list of date time fields it sets. Some primitive
    * input parsers have nothing in their `fields` list. This is fine: it
    * indicates the date times their parsers return are dummy values with no
    * fields set.
    */
  case class PartialParser(fields: List[DateTimeFieldType], parser: Parser[DateTime])
      extends Parser[DateTime] {
    def apply(in: Input) = parser(in)

    /**
      * Sequential composition with another partial file name parser
      *
      * It is a parse error (not failure) if the date time fields are inconsistent.
      */
    def followedBy(other: PartialParser) = {
      val combinedFields = (fields ++ other.fields).distinct
      val combinedParser = for {
        dt1 <- parser
        dt2 <- other.parser
        dt3 <- toParser(combineDateTimes(dt1, fields, dt2, other.fields))
      } yield dt3
      PartialParser(combinedFields, combinedParser)
    }

    /**
      * Combine the information in two date times safely.
      *
      * Given two date times, and a list of fields defined in each date time,
      * return a new date time containing the union of all fields. It is an
      * error if the date times have different values for any field, since
      * parsers should always yield only a single date time, even if some
      * or all components are repeated.
      */
    def combineDateTimes(
      dt1: DateTime, fs1: List[DateTimeFieldType], dt2: DateTime, fs2: List[DateTimeFieldType]
    ): Result[DateTime] =

      // if there are no fields in fs1, just return unmodified dt2
      if (fs1.isEmpty) Result.ok(dt2)

      // if there are fields in fs1, copy fields from fs2 to fs1
      // (if there are no fields in fs2 this will quickly return unmodifed dt1)
      else fs2.foldLeftM[Result, DateTime](dt1)((dtAcc, f) => {
        val field  = dt2.property(f).getField
        val dt2Val = field.get(dt2.getMillis)
        val dt1Val = field.get(dt1.getMillis)
        val inFs1  = fs1.contains(f)
        if      (!inFs1)                    Result.ok(dtAcc.withField(f, dt2Val))
        else if (inFs1 && dt2Val == dt1Val) Result.ok(dtAcc)
        else  /*inFs1 && dt2Val != dt1Val*/ Result.fail(s"conflicting values found for $f")
      })
  }


  /** Succeeds if we are at end of file */
  val eof =
    Parser(in => if (in.atEnd) Success((), in) else Failure("not at EOF", in))

  /**
    * Methods for producing `PatternParser`s for a specific time zone.
    *
    * The important one is `pattern`: this parses the whole file pattern
    * and returns the complete file name parser
    */
  object PatternParser {
    val esc       = '\\'
    val one       = '?'
    val any       = '*'
    val start     = '{'
    val end       = '}'
    val tableSign = "table"

    /** Surround a parser with curly brackets */
    def surround[U](parser: Parser[U]) = accept(start) ~> parser <~ accept(end)

    /** Parse a single character, which may be escaped */
    def escape(specialChars: Char*) = {
      val allSpecial = esc :: specialChars.toList
      val normal  = elem("normal char", !allSpecial.contains(_))
      val special = elem("special char", allSpecial.contains(_))
      val escaped = accept(esc) ~> special
      normal | escaped
    }

    /** Parse the largest possible literal segment */
    def literal(tableName: String): PatternParser = {
      val single = escape(one, any, start, end)   ^^ (_.toString)
      val table  = surround(acceptSeq(tableSign)) ^^ (_ => tableName)
      rep1(single | table) ^^ (lits => PartialParser.literal(lits.mkString))
    }

    /**
      * Parse a timestamp
      *
      * It is an error if the pattern inside curly braces is not a valid
      * timestamp format (except for "{table}", of course).
      */
    def timestamp(tz: DateTimeZone): PatternParser = for {
      tsPattern <- surround(rep(escape(end))) ^^ (_.mkString)
      fnParser  <- if (tsPattern == tableSign) failure("'table' matches the table name, not a date time")
                   else                        toParser(PartialParser.timestamp(tz, tsPattern))
    } yield fnParser

    /** Parse a question mark */
    val unknown: PatternParser =
      accept(one) ^^ (_ => PartialParser.unknown)

    /** Parser which succeeds if we have consumed the entire file pattern */
    val finished: PatternParser =
      eof ^^ (_ => PartialParser.finished)

    /**
      * Parser which consumes a wildcard and the following segment too.
      *
      * The wildcard behaves more strictly than standard glob wildcards.
      * The wildcard matches the shortest possible number of characters required
      * for the subsequent element to succeed. This makes the wildcard slightly
      * less convienient, but hopefully more predictable.
      */
    def unknownSeq(tableName: String, tz: DateTimeZone): PatternParser =
      rep1(accept(any)) ~> (literal(tableName) | timestamp(tz) | unknown | finished) ^^ (PartialParser.anyUntil(_))

    /** Match a single element element */
    def part(tableName: String, tz: DateTimeZone): PatternParser =
      literal(tableName) | timestamp(tz) | unknown | unknownSeq(tableName, tz)

    /** Parses a file pattern, returning the subsequent file name parser */
    def pattern(tableName: String, tz: DateTimeZone): Parser[FileNameParser] = for {
      parts     <- rep(part(tableName, tz))
      _         <- eof
      raw       =  parts.foldRight(PartialParser.finished)(_ followedBy _)
      validated <- toParser(PartialParser.validate(raw))
    } yield validated
  }

  /** Factory for partial file name parsers */
  object PartialParser {

    /** Partial file name parser expecting a literal */
    def literal(lit: String) = PartialParser(List.empty[DateTimeFieldType],
      acceptSeq(lit) ^^ (_ => new DateTime(0))
    )

    /** Input parser for matching any single char */
    val unknown = PartialParser(List.empty[DateTimeFieldType],
      elem("char", _ => true) ^^ (_ => new DateTime(0)
    ))

    /** Partial file name parser that succeeds when at the end of the file name */
    val finished = PartialParser(List.empty[DateTimeFieldType],
      eof ^^ (_ => new DateTime(0))
    )

    /** Input parser for adding an arbitrary sequence of characters in front of another parser */
    def anyUntil(next: PartialParser) = {
      // rhs of | is lazy, so parser does not recurse indefinitely
      def parser: Parser[DateTime] = next.parser | (unknown ~> parser)
      PartialParser(next.fields, parser)
    }

    /**
      * Input parser expecting a timestamp following a joda-time pattern
      *
      * We try to ensure that the parser does not allow joda-time to parse
      * a negative year.
      */
    def timestamp(tz: DateTimeZone, pattern: String): Result[PartialParser] = for {
      formatter <- Result.safe(DateTimeFormat.forPattern(pattern).withZone(tz))
      fields    <- fromOption(DateFormatInfo.fields(formatter),
        s"Could not find fields in date time pattern <$pattern>."
      )
      yearFirst = pattern startsWith "y" // hack to avoid negative years
    } yield PartialParser(fields, Parser(in => in match {
      // if we have underlying string, we can convert DateTimeFormatter.parseInto method into a scala Parser
      case _: CharSequenceReader => {
        if (in.atEnd)
          Failure("Cannot parse date time when at end of input", in)
        else if (yearFirst && in.first == '-')
          Failure("No negative years", in) // hack to avoid negative years
        else {
          val underlying = in.source.toString
          val pos = in.offset
          val mutDateTime = new MutableDateTime(1970, 1, 1, 0, 0, 0, 0, tz)  // Set defaults for fields
          val parseRes = Result.safe(formatter.parseInto(mutDateTime, underlying, pos))
          val dateTime = mutDateTime.toDateTime

          parseRes.foldMessage(newPosOrFailPos =>
            if (newPosOrFailPos >= 0) {  // have newPos
              Success(dateTime, new CharSequenceReader(underlying, newPosOrFailPos))
            } else {
              val failPos = ~newPosOrFailPos
              val beforeStart = underlying.substring(0, pos)
              val beforeFail = underlying.substring(pos, failPos)
              val afterFail = underlying.substring(failPos, underlying.length)
              val msg = s"Failed to parse date time. Date time started at the '@' and failed at the '!' here: $beforeStart @ $beforeFail ! $afterFail"
              Failure(msg, new CharSequenceReader(underlying, failPos))
            }, msg => Error(msg, in)
          )
        }
      }

      // if we don't have underlying string, we're hosed
      case _ => Error(s"PartialParser only works on CharSequenceReaders", in)
    }))

    /** The (maximum) frequencies that should be inferred for corresponding date-time fields */
    val frequencies = Vector(
      Period.years(1), Period.months(1), Period.days(1), Period.hours(1), Period.minutes(1), Period.seconds(1)
    )

    /**
      * Turns a PartialParser into a FileNameParser.
      *
      * Checks that the fields in the file pattern are valid. The restrictions
      * on file pattern fields are described in [[au.com.cba.omnia.maestro.task.UploadExecution]].
      */
    def validate(parser: PartialParser): Result[FileNameParser] = for {
      _           <- Result.guard(parser.fields.nonEmpty, s"no date time fields found")
      fieldOrders <- parser.fields.traverse[Result, Int](fieldOrder(_))
      dirFuncs    <- (0 to fieldOrders.max).toList.traverse[Result, DateTime => String](i =>
        Result
          .guard(fieldOrders contains i, s"missing date time field ${uploadFields(i)}")
          .map(_ => dateTime => {
            val field = uploadFields(i)
            val value = dateTime.property(field).getField.get(dateTime.getMillis)
            if (field equals DateTimeFieldType.year) f"$value%04d" else f"$value%02d"
          })
      )
    } yield parser ^^ (dateTime => (dirFuncs map (_(dateTime)), dateTime, frequencies(fieldOrders.max)))

    /** Ordered list of fields we permit in upload time stamps */
    val uploadFields = List(
      DateTimeFieldType.year,
      DateTimeFieldType.monthOfYear,
      DateTimeFieldType.dayOfMonth,
      DateTimeFieldType.hourOfDay,
      DateTimeFieldType.minuteOfHour,
      DateTimeFieldType.secondOfMinute
    )

    /** Assign numbers to each field that upload works with */
    def fieldOrder(t: DateTimeFieldType): Result[Int] = {
      val index = uploadFields indexOf t
      if (index >= 0) Result.ok(index)
      else            Result.fail(s"Upload does not accept $t fields")
    }
  }
}
