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

import scala.util.parsing.combinator.RegexParsers
import scala.util.parsing.input.{Position, Positional}

/**
 * Lexer for schema spec tables.
 *
 * The lexer converts a string into a list of tokens, as the first stage in
 * parsing a schema spec table.
 * It can be called as follows:
 * {{{
 *  import TableLexer._
 *  val input: String
 *  val tokens: Either[NoSuccess, List[PositionalToken[_]]] = TableLexer(input)
 * }}}
 *
 * Tokens are described using a GADT defined within the lexer object.
 * `PositionalTokens` contain both the token and a position within the file
 *  at which the token was encountered.
 *
 * The process for adding new tokens is as follows:
 *  1. Determine the top-level trait under which the token belongs (Token,
 *     TPunct, TKeyword, TName or TLiteral) and add it as either a case
 *     class or case object under this trait.
 *  2. If the token is punctuation or a keyword, add it to the relevant
 *     map (punctMap or kwMap). This automatically provides a toString()
 *     implementation, a parser and some testing capabilities.
 *  3. For other token types, create parsers, create an entry in
 *     tokensToString and add them to the final sum parser pTokens.
 *  4. Add tests within TableLexerSpec.
 */
object Lexer extends RegexParsers {

  // Base trait for all tokens
  sealed trait Token {
    override def toString: String = tokenToString(this)  
  }

  // Base trait for all "simple" tokens that take no arguments
  sealed trait SimpleToken          extends Token

  // Punctuation
  sealed trait TPunct               extends SimpleToken
  case object KBra                  extends TPunct
  case object KKet                  extends TPunct
  case object KSemi                 extends TPunct
  case object KPipe                 extends TPunct
  case object KColon                extends TPunct
  case object KComma                extends TPunct
  case object KPeriod               extends TPunct
  case object KDash                 extends TPunct
  case object KEquals               extends TPunct
  case object KPlus                 extends TPunct

  // Keywords
  sealed trait TKeyword             extends SimpleToken
  case object KTable                extends TKeyword
  case object KAnd                  extends TKeyword
  case object KIs                   extends TKeyword
  case object KNull                 extends TKeyword
  case object KIgnore               extends TKeyword

  // Names
  sealed trait TName                extends Token

  // type constructor, starting with an upper case letter.
  case class KCtor(ctor: String)    extends TName

  // name of table or field, starting with a lower case letter.
  case class KName(name: String)    extends TName

  // Literals
  sealed trait TLiteral             extends Token
  case class KString(str: String)   extends TLiteral
  case class KChar(chr: Char)       extends TLiteral
  case class KNat(nat: Int)         extends TLiteral

  // Comment
  case class KComment(comment: String) extends Token

  // PositionalToken contains both a token and position information
  case class PositionalToken[+T <: Token](token: T, pos: Position)

  // Apply the lexer to a string, producing a list of tokens.
  def apply(input: String): Either[NoSuccess, List[PositionalToken[Token]]] =
    parseAll(pTokens, input) match {
      case Success(result, _)  => Right(result.map(toPosToken))
      case failure : NoSuccess => Left (failure)
    }

  /**
   * Converts a token to its string representation.
   * This is essentially the reverse of the lexing process.
   * @param t token to convert to a string
   * @return string representation of the token
   */
  def tokenToString(t: Token): String = t match {
    case p: TPunct         => punctMap(p).toString
    case k: TKeyword       => kwMap(k)
    case KCtor(ctor)       => ctor
    case KName(name)       => name
    case KString(str)      => "'" + str + "'"
    case KChar(chr)        => s"'$chr'"
    case KNat(nat)         => nat.toString
    case KComment(comment) => s"-- $comment\n"
  }

  /** Punctuation token set. */
  lazy val punctuation: Set[TPunct] = punctMap.keys.to[Set]

  /** Keyword token set. */
  lazy val keywords: Set[TKeyword] = kwMap.keys.to[Set]

  // punctuation tokens and their characters
  private val punctMap: Map[TPunct, Char] = Map(
    KBra        -> '(',
    KKet        -> ')',
    KSemi       -> ';',
    KPipe       -> '|',
    KColon      -> ':',
    KComma      -> ',',
    KPeriod     -> '.',
    KDash       -> '-',
    KEquals     -> '=',
    KPlus       -> '+'
  )

  // keyword tokens and their strings
  private val kwMap: Map[TKeyword, String] = Map(
    KTable      -> "%table",
    KIgnore     -> "%ignore",
    KAnd        -> "and",
    KIs         -> "is",
    KNull       -> "null"
  )


  // --------------------------------------------------------------------------
  /* InternPosToken contains both a token and position information
   * Position in the PosToken is mutable, which is inherited from the
   * scala.util.parsing.input.Positional trait. We produce a sequence
   * of our own PositionalTokens to give up the mutable position.
   */
  case class PosToken[+T <: Token](token: T) extends Positional

  def toPosToken[T <: Token](i: PosToken[T]): PositionalToken[T] =
    PositionalToken(i.token, i.pos)

  // Parse a comment. Comments are Haskell-style, single line comments.
  def pComment: Parser[PosToken[KComment]] = positioned(
    // Empty comment.
    """--[\s^\n]*\n""".r   ^^^ { PosToken(KComment(""))      } |

    // Regular comment.
    "--" ~> """[^\n]*""".r ^^  { s => PosToken(KComment(s))  })

  // Parse a punctuation character.
  def pPunctuation: Parser[PosToken[TPunct]] = positioned(
    punctMap
      .iterator
      .map { case (token, c) => c ^^^ PosToken(token) }
      .reduce(_ | _))

  // Parse a positioned keyword.
  def pKeywords: Parser[PosToken[TKeyword]] = positioned(
    kwMap
      .iterator
      .map { case (token, s) => pKeyword(s) ^^^ PosToken(token) }
      .reduce(_ | _))

  // Parse a single keyword.
  def pKeyword(s: String): Parser[String] 
    = guard((s + "[\\W]").r) ~> s | s <~ pEnd

  // Match the end of input.
  def pEnd: Parser[Unit] = Parser { in: Input =>
    if (in.atEnd) Success((), in) else Failure("not at end of input", in)
  }

  // Parse a constructor (starting with an uppercase letter) 
  // or name (starting with a lowercase letter).
  def pNames: Parser[PosToken[TName]] = positioned(
    ("""[A-Z]\w*""".r      ^^ { s => PosToken(KCtor(s)) }) |
    ("""[a-z]\w*""".r      ^^ { s => PosToken(KName(s)) })
  )

  // Parse a literal value.
  def pLiterals: Parser[PosToken[TLiteral]] = positioned(
    ("\"[^\"]*\"".r        ^^ { s => PosToken(KString(trimEnds(s))) }) |
    ("'[^']*'".r           ^^ { s => PosToken(KString(trimEnds(s))) }) |
    ('\'' ~> ".".r <~ '\'' ^^ { s => PosToken(KChar  (s.head))      }) |
    ("[0-9]+".r            ^^ { s => PosToken(KNat   (s.toInt))     })
  )

  // Trim end characters from a string.
  def trimEnds(s: String) 
    = s.drop(1).dropRight(1)

  // Parse a token.
  def pTokens: Parser[List[PosToken[Token]]] =
    ( pComment
    | pPunctuation
    | pKeywords
    | pNames
    | pLiterals
    ).*
}

