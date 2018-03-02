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

// Basic syntaxes that are not associated with a particular tope.
package au.com.cba.omnia.maestro.schema
package syntax

import  au.com.cba.omnia.maestro.schema._


/** Matches everything. */
object Any   extends Syntax {

  val name  = "Any"
  override def sortOrder  = 0
  def likeness(s: String) = 1.0

  val parents:    Set[Syntax] = Set()
  val partitions: Set[Syntax] = Set()
}


/** Hive encoding of Null values. */
object Null   extends Syntax {

  val name  = "Null"
  override def sortOrder = 1

  def likeness(s: String) = 
    if (s == "\\N") 1.0 else 0.0

  val parents:    Set[Syntax]   = Set(Any)
  val partitions: Set[Syntax]   = Set()
  override val isIsolate: Boolean = true
}


/** Entirely empty string. */
object Empty  extends Syntax {

  val name  = "Empty"
  override def sortOrder = 2

  def likeness(s: String) = 
    if (s.length == 0) 1.0 else 0.0

  val parents:    Set[Syntax]   = Set(Any)
  val partitions: Set[Syntax]   = Set()
  override val isIsolate: Boolean = true
}


/** Whitespace only. */
object White  extends Syntax {

  val name  = "White"
  override def sortOrder = 3

  def likeness(s: String) = 
    if  (s.length == 0) 0.0
    else s.filter(_.isWhitespace).length / s.length

  val parents:    Set[Syntax]   = Set(Any)
  val partitions: Set[Syntax]   = Set()
  override val isIsolate: Boolean = true
}


/** Alphanumeric characters. */
object AlphaNum extends Syntax {

  val name  = "AlphaNum"
  override def sortOrder = 4

  def likeness(s: String) = 
    if  (s.length == 0) 0.0
    else s.filter(c => c.isLower || c.isUpper || c.isDigit)
          .length / s.length

  val parents:    Set[Syntax] = Set(Any)
  val partitions: Set[Syntax] = Set()
}


/** Alphabetic characters. */
object Alpha extends Syntax {

  val name  = "Alpha"
  override def sortOrder = 5

  def likeness(s: String) = 
    if  (s.length == 0) 0.0
    else s.filter(c => c.isLower || c.isUpper)
          .length / s.length

  val parents:    Set[Syntax] = Set(AlphaNum)
  val partitions: Set[Syntax] = Set()
}


/** Upper case characters. */
object Upper extends Syntax {

  val name  = "Upper"
  override def sortOrder = 6

  def likeness(s: String) = 
    if  (s.length == 0) 0.0
    else s.filter(_.isUpper).length / s.length

  val parents:    Set[Syntax] = Set(Alpha)
  val partitions: Set[Syntax] = Set()
}


/** Lower case characters. */
object Lower extends Syntax {

  val name  = "Lower"
  override def sortOrder = 7

  def likeness(s: String) = 
    if  (s.length == 0) 0.0
    else s.filter(_.isLower).length / s.length

  val parents:    Set[Syntax] = Set(Alpha)
  val partitions: Set[Syntax] = Set()
}


/** An integer. */
object Integer extends Syntax {

  val name  = "Int"
  override  def sortOrder = 8

  def likeness(s: String) = 
    if      (s.length == 0) 0.0
    else if (s(0) == '-')
          NegInt.likeness(s)
    else     Nat.likeness(s)
    
  val parents:    Set[Syntax] = Set(Real)
  val partitions: Set[Syntax] = Set(Nat, NegInt)
}


/** Natural number/positive integer Just digits. */
object Nat extends Syntax {

  val name  = "Nat"
  override def sortOrder = 9

  def likeness(s: String) = 
    if  (s.length == 0) 0.0 
    else s.filter(_.isDigit).length / s.length

  val parents:    Set[Syntax] = Set(AlphaNum, PosReal, Integer)
  val partitions: Set[Syntax] = Set()
}


/** Negative integers. */
object NegInt extends Syntax {

  val name = "NegInt"
  override def sortOrder = 10

  def likeness(s: String) = 
    if      (s.length == 0) 0.0
    else if (s(0) != '-')   0.0
    else Nat.likeness(s.substring(1, s.length))

  val parents:    Set[Syntax] = Set(Integer, NegReal)
  val partitions: Set[Syntax] = Set()
}


/** A real number,
    with floating-point notation like (12345.678) or (-12345.678). */
object Real extends Syntax {

  val name  = "Real"
  override def sortOrder = 11

  def likeness(s: String) = 
    if      (s.length == 0) 0.0
    else if (s(0) == '-')   
          NegReal.likeness(s)
    else  PosReal.likeness(s)                      

  val parents:    Set[Syntax] = Set(Any)
  val partitions: Set[Syntax] = Set(NegReal, PosReal)
}


/** A positive real number,
    with floating-point notation like (12345.678). */
object PosReal extends Syntax {

  val name  = "PosReal"
  override def sortOrder = 12

  def likeness(s: String): Double = 
    if      (s.length == 0)            0.0
    else if (!s(0)           .isDigit) 0.0
    else if (!s(s.length - 1).isDigit) 0.0

    else {
      val nDigits = s.filter(_.isDigit).length
      val nPoints = s.filter(c => c == '.').length

      if ((nDigits == s.length) || 
          (nDigits == (s.length - 1) && nPoints == 1)) 
           1.0
      else 0.0
    }

  val parents:    Set[Syntax] = Set(Real)
  val partitions: Set[Syntax] = Set()
}


/** A negative real number,
    with floating-point notation like (-12345.678). */
object NegReal extends Syntax {

  val name  = "NegReal"
  override def sortOrder = 13

  def likeness(s: String): Double = 
    if      (s.length == 0)   0.0
    else if (s(0) != '-')     0.0
    else PosReal.likeness(s.substring(1, s.length))

  val parents:    Set[Syntax] = Set(Real)
  val partitions: Set[Syntax] = Set()
}


/** Matches this exact string. */
case class Exact(str: String) extends Syntax {
  
  val name  = s"""Exact('$str')"""
  override def sortOrder = 99

  def likeness(s: String) = 
    if (s == str) 1.0 else 0.0

  // Try to attach an exact string to a likely place in the tree.
  val parents: Set[Syntax] = 
    if      (Nat.matches(str))              Set(Nat)
    else if (Upper.matches(str))            Set(Upper)
    else if (Lower.matches(str))            Set(Lower)
    else if (PosReal.matches(str))          Set(PosReal)
    else if (YYYYcMMcDD("-").matches(str))  Set(YYYYcMMcDD("-"))
    else Set(Any)

  val partitions: Set[Syntax] = Set()
}

