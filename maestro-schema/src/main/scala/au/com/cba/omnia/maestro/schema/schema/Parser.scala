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

import scala.collection.immutable._
import scala.util.parsing.combinator.Parsers
import scala.util.parsing.input.{NoPosition, Position, Reader}

import au.com.cba.omnia.maestro.schema.syntax._
import au.com.cba.omnia.maestro.schema.hive._
import au.com.cba.omnia.maestro.schema.hive.HiveType._

import Lexer._


/** Base trait for column entries that are ignored. */
sealed trait Ignore

/** Ignore entries in a field that are equal to some value.
 *
 *  Encoded as
 *  {{{
 *    name = "VALUE"
 *  }}}
 *  in the text format.
 *
 *
 *  @param field field to check
 *  @param value value to ignore in this field
 */
case class IgnoreEq(field: String, value: String) extends Ignore

/** Ignore entries in a field that are equal to null.
 *
 *  Encoded as
 *  {{{
 *    name is null
 *  }}}
 *  in the text format.
 *
 * @param field field to ignore when it is null
 */
case class IgnoreNull(field: String) extends Ignore


/**
 * Parser for table schemas.
 *
 * The parser operates on a sequence of Lexer.PositionalTokens.  It can also be
 * invoked on a string, which will first convert the string to a sequence of
 * tokens using the Lexer:
 * {{{
 *   import Parser._
 *   val input: String
 *   val table: NoSuccess \/ Schema = Parser(input)
 * }}}
 * When parsing the table fails, the message stored in the result may contain
 * useful information regarding the location of the error, which can be reported
 * to the user.
 */
object SchemaParser extends Parsers {

  /** Element type (we parse Lexer tokens). */
  type Elem = PositionalToken[Token]


  // --------------------------------------------------------------------------
  /** Parse a SchemTable from a String. */
  def apply(input: String): Either[NoSuccess, TableSpec] =
    parseString(pSchemaTable, input)

  /** Parse a SchemaTable from some tokens. */
  def apply(input: List[Elem]): Either[NoSuccess, TableSpec] =
    parseElems(pSchemaTable, input)


  // --------------------------------------------------------------------------
  /** Parse a thing from a string. */
  def parseString[A](parser: Parser[A], input: String)
    : Either[NoSuccess, A] =
    Lexer(input) match {
      case Left(Lexer.Error   (msg, nextChar))
        => Left(Error  (msg, SequenceReader.empty))

      case Left(Lexer.Failure (msg, nextChar))
        => Left(Failure(msg, SequenceReader.empty))

      case Right(tokens) => parseElems(parser, tokens)
    }

  /** Parse a thing from some tokens */
  def parseElems [A](parser: Parser[A], input: List[Elem])
    : Either[NoSuccess, A] =
    phrase(parser)(new SequenceReader(input)) match {
      case Success(result, _)  => Right(result)
      case failure : NoSuccess => Left (failure) }


  // ---------------------------------------------------------------------------
  // Parse a %table expression: %table dbname.tablename;
  case class TableId(database: String, table: String)

  lazy val pTableExpr: Parser[TableId] =
    (pTok(KTable)
      ~! (pName         | err("expected database name (eg: databaseName.tableName)"))
      ~! (pTok(KPeriod) | err("expected period after database name"                ))
      ~! (pName         | err("expected table name (eg: databaseName.tableName)"   ))
      ~! (pTok(KSemi)   | err("expected ';' after table declaration"               ))
    ) ^^ { case _ ~ database ~ _ ~ table ~ _ => TableId(database, table) }


  // --------------------------------------------------------------------------
  // Parse a whole table specification.
  def pSchemaTable: Parser[TableSpec] =
    pTableExpr ~! (pIgnoreExpr?) ~! (pColumnSpec.*) ^^ {
      case tableId ~ optIgnores ~ columnSpecs =>
        TableSpec(
          tableId.database,
          tableId.table,
          optIgnores.getOrElse(Seq.empty[Ignore]).toList,
          columnSpecs
        )}


  // --------------------------------------------------------------------------
  // Parse a ColumnSpec.
  def pColumnSpec: Parser[ColumnSpec] =
    (pName
      ~! (pTok(KPipe) | err("expected pipe | symbol after column name"  ))
      ~! (pHiveType   | err("expected hive type"                        ))
      ~! (pTok(KPipe) | err("expected pipe | symbol after hive type "   ))
      ~! (pOptFormat  | err("expected format or dash"                   ))
      ~! (pTok(KPipe) | err("expected pipe | symbol after format"       ))
      ~! (pHistogram  | err("expected histogram"                        ))
      ~! (pTok(KSemi) | err("expected ';' after column specification"   ))
    ) ^^ { case name ~ _ ~ hivetype ~ _ ~ topeSyntax ~ _ ~ histogram ~ _ =>
            ColumnSpec(name, hivetype, topeSyntax, histogram, "") }


  // --------------------------------------------------------------------------
  // Ignore Expressions
  def pIgnoreEq =
    (pName
      ~  pTok(KEquals)
      ~! (pString | err("expected string literal for the field to ignore (eg: name='VALUE')"))
    ) ^^ { case field ~ _ ~ value => IgnoreEq(field, value) }

  def pIgnoreNull =
    (pName
      ~   pTok(KIs)
      ~! (pTok(KNull) | err("expected null (eg: name is null)"))
    ) ^^ { case field ~ _ ~ _ => IgnoreNull(field) }

  def pIgnoreSingleTerm =
    pIgnoreEq | pIgnoreNull | err("expected %ignore term")

  // Parse an %ignore expression: %ignore name = 'VALUE' and otherName is null and ... ;
  def pIgnoreExpr: Parser[Seq[Ignore]] =
    (pTok(KIgnore)
      ~! repsep(pIgnoreSingleTerm, pTok(KAnd))
      ~! (pTok(KSemi) | err("';' expected to complete %ignore declaration"))
    ) ^^ { case _ ~ ignoreSeq ~ _ => ignoreSeq }


  // --------------------------------------------------------------------------
  // Formats

  // Parse an optional format (a missing format is indicated using a dash symbol)
  def pOptFormat: Parser[Option[Format]]
    = ( pTok(KDash)   ^^^ None
      | pFormat       ^^ { t: Format => Some(t) }
      | err("unknown tope"))

  // Parse a format, which is either a tope with its syntax or a plain syntax.
  def pFormat:  Parser[Format]
    = ( pClassifier ~ ( pTok(KPlus) ~ pFormat ^^ { case _ ~ more => more }
                      | success(Format(List())))
      ) ^^ { case c ~ more => Format(List(c)) ++ more }

  def pClassifier: Parser[Classifier]
    = ( pTopeSyntax   ^^ { x: (Tope, Syntax) => x match {
          case (t, s) => ClasTope(t, s)  }}

      | pSyntax       ^^ { x: Syntax          =>  ClasSyntax(x) })

  // Parse a tope with its syntax.
  def pTopeSyntax: Parser[(Tope, Syntax)]
    = ( pTopeSyntaxGeneric
      | pTopeSyntaxDay)

  // Parse a generic tope and syntax, by asking the tope what its syntaxes
  // are directly.
  def pTopeSyntaxGeneric: Parser[(Tope, Syntax)]
    = Topes.topeSyntaxes
      .toSeq
      .map { case (tope, syntax)
              =>  (psCtor(tope.name) ~ pTok(KPeriod) ~
                    psCtor(syntax.name)) ^^^ ((tope, syntax)) }
      .reduce(_ | _)

  // Parse a Day tope, with one of its syntaxes.
  def pTopeSyntaxDay:     Parser[(Tope, Syntax)]
    = ( (psCtor("Day") ~ pTok(KPeriod) ~ pYYYYcMMcDD) ^^
          { case _ ~ _ ~ syntax => (tope.Day, syntax) }

      | (psCtor("Day") ~ pTok(KPeriod) ~ pDDcMMcYYYY) ^^
          { case _ ~ _ ~ syntax => (tope.Day, syntax) })


  // --------------------------------------------------------------------------
  // Syntaxes

  // Simple syntaxes don't contain arguments; exact syntaxes are Exact("string")
  // TODO: split this.
  val (simpleSyntaxes, exactSyntaxes): (Seq[Syntax], Seq[Exact]) = {
    val syns: Seq[Syntax] = Syntaxes.syntaxes.to[Seq]
    val (ss, es) = syns.partition {
      case _ : Exact => false
      case _         => true
    }
    (ss, es.map{ case e: Exact => e })
  }

  // Parse a Syntax.
  def pSyntax: Parser[Syntax]
    = ( pSimpleSyntax
      | pExactSyntax
      | pYYYYcMMcDD
      | pDDcMMcYYYY)

  // Parse a simple syntax.
  def pSimpleSyntax: Parser[Syntax]
    = simpleSyntaxes
      .map { syntax: Syntax => psCtor(syntax.name) ^^^ syntax }
      .reduce(_ | _)

  // Parse an exact syntax.
  def pExactSyntax: Parser[Exact]
    = exactSyntaxes
      .map{ exact: Exact
        => psCtor("Exact") ~> pTok(KBra) ~> psString(exact.str) <~ pTok(KKet) ^^^
            Exact(exact.str)}
      .reduce(_ | _)

  // Parse a year-month-day syntax specifier.
  def pYYYYcMMcDD:  Parser[Syntax]
    = psCtor("YYYYcMMcDD") ~! pTok(KBra) ~! pString ~! pTok(KKet) ^^
        { case _ ~ _ ~ s ~ _ => syntax.YYYYcMMcDD(s) }

  // Parse a day-month-year syntax specifier.
  def pDDcMMcYYYY:  Parser[Syntax]
    = psCtor("DDcMMcYYYY") ~! pTok(KBra) ~! pString ~! pTok(KKet) ^^
        { case _ ~ _ ~ s ~ _ => syntax.DDcMMcYYYY(s) }


  // --------------------------------------------------------------------------
  // Parse a sequence of Histograms.
  //  This is what we get from tasting a table.
  def pTaste: Parser[List[Histogram]]
    = rep(pTasteColumn)

  // Taste of a single column.
  def pTasteColumn: Parser[Histogram]
    = pHistogram <~ pTok(KSemi)


  // --------------------------------------------------------------------------
  // Parse a Histogram.
  def pHistogram: Parser[Histogram]
    = repsep(pItem, pTok(KComma)) ^^
        { case ls : List[(Classifier, Int)] => Histogram(ls.toMap) }

  def pItem: Parser[(Classifier, Int)]
    = (pClassifier
        ~! (pTok(KColon) | err("expected colon after tope or syntax name"))
        ~! (pNat         | err("expected the number of occurrences of the tope"))
      ) ^^ { case clas ~ _ ~ number => (clas, number) }


  // --------------------------------------------------------------------------
  // Parsers that match on individual tokens

  // Parse a literal token.
  def pTok(token : SimpleToken) : Parser[Unit]
    = extract { case `token` => () }

  // Parse a constructor.
  def pCtor   : Parser[String]
    = extract { case KCtor(ctor) => ctor }

  // Parse a specific constructor.
  def psCtor(s: String): Parser[String]
    = pCtor.filter(_ == s)

  // Parse a name.
  def pName   : Parser[String]
    = extract { case KName(name) => name }

  // Parse a specific name.
  def psName(s: String): Parser[String]
    = pName.filter(_ == s)

  // Parse a Hive storage type, like 'string' or 'double'.
  def pHiveType : Parser[HiveType]
    = extract {
        case KName("string")   => HiveType.HiveString
        case KName("double")   => HiveType.HiveDouble
        case KName("int")      => HiveType.HiveInt
        case KName("bigint")   => HiveType.HiveBigInt
        case KName("smallint") => HiveType.HiveSmallInt }

  // Parse a literal string in double quotes, like "foo".
  def pString : Parser[String]
    = extract { case KString(string) => string }

  // Parse a natural number (positive integer) like 42.
  def pNat    : Parser[Int]
    = extract { case KNat(n)  => n }

  // Parsers for particular instances of literals
  def psString(s: String): Parser[String]
    = pString.filter(_ == s)


  /**
   * Creates a parser based on a partial function than can extract a value from
   * a Token.
   *
   * The token stream consists of either case objects or case classes.
   * Case objects may be extracted with a Unit parser:
   * {{{
   *   val p: Parser[Unit] = extract { case SomeCaseObject => () }
   * }}}
   * This produces a parser that explicitly matches only on the case object,
   * but does not need to produce any other value.
   *
   * Case class tokens typically contain a single string parameter, such as the
   * name of a constructor.  These can be extracted using a String parser:
   * {{{
   *   val p: Parser[String] = extract { case SomeCaseClass(name) => name }
   * }}}
   * This parser also explicitly matches only on the case class, and extracts
   * its provided parameter as a string.
   *
   * @param f partial function from Token to T, defined when a Token can provide T
   * @tparam T type of the parser
   * @return new parser which extracts a T from a Token
   */
  def extract[T](f: PartialFunction[Token, T]): Parser[T] =
    acceptIf { p => f.isDefinedAt(p.token)         }
             { p => s"Unexpected token ${p.token}" }
      .map { p => f(p.token) }


  /**
   * Reader of a sequence of PositionalTokens.
   *
   * The reader is used as input to all parsers of PositionalTokens.
   *
   * The previous position is threaded through the reader as a best-guess for when
   * a token position cannot be determined.  This happens when the last token in a
   * stream causes an error, because `s.headOption` does not exist.
   *
   * @param s sequence of PositionalTokens to read
   * @param prev previous position of the reader (used when the end of stream is reached)
   */
  class SequenceReader(s: Seq[Elem], prev: Position = NoPosition)
    extends Reader[Elem]
  {
    def atEnd: Boolean      = s.isEmpty
    def first: Elem         = s.head
    def pos:   Position     = s.headOption.fold(prev)(_.pos)
    def rest:  Reader[Elem] = if (atEnd) this else new SequenceReader(s.tail, pos)
  }

  object SequenceReader {
    lazy val empty: SequenceReader = new SequenceReader(Seq.empty[Elem])
  }
}
