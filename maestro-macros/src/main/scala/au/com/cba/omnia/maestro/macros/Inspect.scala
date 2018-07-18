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

import scala.reflect.macros.whitebox.Context

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.humbug.HumbugThriftStruct

object Inspect {
  /** Regex to match Scala `Product` style field names. */
  val ProductField = """_(\d+)""".r

  /**
    * For a given ThriftStruct returns information about each field.
    * 
    * For each field it gives the Scala product index for the field, the field name using as it is
    * in the generated code (camel case) for the thrift struct and the MethodSymbol representing the
    * getter for the field.
    */
  def info[A <: ThriftStruct : c.WeakTypeTag](c: Context): List[(Int, String, c.universe.MethodSymbol)] =
    infoUnsafe(c)(c.universe.weakTypeOf[A])

  /** Same as `info` but for any type where the type is assumed to be ThriftStruct.*/
  def infoUnsafe(c: Context)(typ: c.universe.Type): List[(Int, String, c.universe.MethodSymbol)] = {
    import c.universe._

    ensureThriftTypeUnsafe(c)(typ)

    val fieldNames =
      if (typ <:< c.universe.weakTypeOf[HumbugThriftStruct]) {
        // Get the fields in declaration order
        typ.decls.sorted.toList.collect {
          case sym: TermSymbol if sym.isVar => sym.name.toString.trim
        }
      } else
        typ.typeSymbol.companion.typeSignature
          .member(TermName("apply")).asMethod.paramLists.head.map(_.name.toString)


    val fieldGetters = typ.members.toList.map(member => (member, member.name.toString)).collect {
      case (member, ProductField(n)) =>
        (n.toInt, member.asMethod)
    }.sortBy(_._1)

    if (fieldNames.length != fieldGetters.length) c.abort(
      c.enclosingPosition,
      s"Error trying to get the field info for $typ. Got these field names $fieldNames which does not match the getters $fieldGetters"
    )

    fieldNames.zip(fieldGetters)
      .map { case (name, (idx, method)) => (idx, name, method) }
      .sortBy(_._1)
  }

  /** For a given ThriftStruct returns a map of each fields name to the getter method. */
  def fieldsMap[A <: ThriftStruct : c.WeakTypeTag](c: Context): Map[String, c.universe.MethodSymbol] =
    fieldsMapUnsafe(c)(c.weakTypeOf[A])

  /** Same as `fieldsMap` but for any type where the type is assumed to be ThriftStruct.*/
  def fieldsMapUnsafe(c: Context)(typ: c.universe.Type): Map[String, c.universe.MethodSymbol] =
    infoUnsafe(c)(typ).map { case (_, n, f) => (n, f) }.toMap

  /** Does a type derive from ThriftStruct? */
  def isThriftType[A : c.WeakTypeTag](c: Context): Boolean =
    c.universe.weakTypeOf[A] <:< c.universe.weakTypeOf[ThriftStruct]

  /** Does a type derive from ThriftStruct? */
  def isThriftTypeUnsafe(c: Context)(typ: c.universe.Type): Boolean =
    typ <:< c.universe.weakTypeOf[ThriftStruct]

  /** Aborts unless `A`` derives from ThriftStruct. */
  def ensureThriftType[A : c.WeakTypeTag](c: Context): Unit =
    // Since type bounds on macro implementations don't currently prevent the macro from expanding
    // we need this workaround to avoid materializing encoders for primitives.
    if (!isThriftType[A](c))
      c.abort(c.enclosingPosition, s"${c.universe.weakTypeOf[A].toString} does not derive ThriftStruct.")

  /** Aborts unless `A` derives from ThriftStruct. */
  def ensureThriftTypeUnsafe(c: Context)(typ: c.universe.Type): Unit =
    // Since type bounds on macro implementations don't currently prevent the macro from expanding
    // we need this workaround to avoid materializing encoders for primitives. 
    if (!isThriftTypeUnsafe(c)(typ))
      c.abort(c.enclosingPosition, s"${typ.toString} does not derive ThriftStruct.")

  /** Does a tupe derive from `HumbugThriftStruct`? */
  def isHumbug[A : c.WeakTypeTag](c: Context): Boolean =
    c.universe.weakTypeOf[A] <:< c.universe.weakTypeOf[HumbugThriftStruct]

  /** Does a tupe derive from `HumbugThriftStruct`? */
  def isHumbugUnsafe(c: Context)(typ: c.universe.Type): Boolean =
    typ <:< c.universe.weakTypeOf[HumbugThriftStruct]

  /**
    * Creates the code to construct a thrift struct of the specified type given code to generate
    * values for each field where the code in the provided fields corresponds to the order of fields
    * in the thrift struct.
    */
  def constructIndexed[A <: ThriftStruct : c.WeakTypeTag](c: Context)(fields: List[c.universe.Tree]): c.universe.Tree =
    constructIndexedUnsafe(c)(c.universe.weakTypeOf[A], fields)

  /**
    * Creates the code to construct a thrift struct of the specified type given code to generate
    * values for each field where the code in the provided fields corresponds to the order of fields
    * in the thrift struct.
    */
  def constructIndexedUnsafe(c: Context)(typ: c.universe.Type, fields: List[c.universe.Tree]): c.universe.Tree =
    if (isHumbugUnsafe(c)(typ)) constructHumbugUnsafe(c)(typ, fields)
    else                        constructScroogeUnsafe(c)(typ,fields)

  /**
    * Creates the code to construct a thrift struct of the specified type given code to generate
    * values for each named field.
    */
  def constructNamed[A <: ThriftStruct : c.WeakTypeTag](c: Context)(fields: List[(String, c.universe.Tree)]): c.universe.Tree =
    constructNamedUnsafe(c)(c.universe.weakTypeOf[A], fields)

  /**
    * Creates the code to construct a thrift struct of the specified type given code to generate
    * values for each named field.
    */
  def constructNamedUnsafe(c: Context)(typ: c.universe.Type, fields: List[(String, c.universe.Tree)]): c.universe.Tree = {
    val order  = Inspect.infoUnsafe(c)(typ).map { case (i, name, _) => (name, i) }.toMap
    val sorted = fields.map { case (n, f) => (order(n), f)}.sortBy(_._1).map(_._2)

    constructIndexedUnsafe(c)(typ, sorted)
  }

  /**
    * Creates the code to construct a Humbug thrift struct of the specified type given code to
    * generate values for each field.
    */
  def constructHumbugUnsafe(c: Context)(typ: c.universe.Type, fields: List[c.universe.Tree]): c.universe.Tree = {
    import c.universe._

    ensureThriftTypeUnsafe(c)(typ)
    val dst = TermName(c.freshName)
    val assignments = fields.zipWithIndex.map { case (f, i) =>
      q"""$dst.${TermName(s"_${i+1}")} = $f"""
    }

    val numFields = Inspect.infoUnsafe(c)(typ).length
    if (fields.length != numFields)
      c.abort(
        c.enclosingPosition,
        s"Can't construct $typ from provided fields. Got code for ${fields.length} fields but needed $numFields"
      )

    q"""
      val $dst = new $typ()
      ..$assignments
      $dst
    """
  }

  /**
    * Creates the code to construct a Scrooge thrift struct of the specified type given code to
    * generate values for each field.
    */
  def constructScroogeUnsafe(c: Context)(typ: c.universe.Type, fields: List[c.universe.Tree]): c.universe.Tree = {
    import c.universe._

    ensureThriftTypeUnsafe(c)(typ)
    val companion = typ.typeSymbol.companion
    q"$companion.apply(..$fields)"
  }
}
