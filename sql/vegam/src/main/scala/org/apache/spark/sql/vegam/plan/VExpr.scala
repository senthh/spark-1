/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.vegam.plan

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

/**
 * Closed arithmetic / column expression used as an aggregate input, a project
 * output, or a join key. Encoded as an s-expression so both backends share it.
 * A column is `{name}`; an ordinal into a shuffle row is `{#n}`.
 */
sealed trait VExpr extends Serializable

case class VCol(name: String) extends VExpr
case class VLong(v: Long) extends VExpr
case class VDouble(v: Double) extends VExpr
case class VStr(v: String) extends VExpr
case class VNull() extends VExpr
case class VCast(child: VExpr, tpe: String) extends VExpr
case class VUnscaled(child: VExpr) extends VExpr
case class VNeg(child: VExpr) extends VExpr
case class VBin(op: String, left: VExpr, right: VExpr) extends VExpr
case class VCoalesce(kids: Seq[VExpr]) extends VExpr
case class VIsNull(child: VExpr) extends VExpr
case class VSubstr(child: VExpr, pos: Int, len: Int) extends VExpr

object VExpr {

  def parse(e: Expression): Option[VExpr] = e match {
    case a: Attribute => Some(VCol(a.name))
    case Alias(c, _) => parse(c)
    case c: Cast => parse(c.child).map(ch => VCast(ch, c.dataType.simpleString))
    case u: UnscaledValue => parse(u.child).map(VUnscaled)
    case UnaryMinus(c, _) => parse(c).map(VNeg)
    case Add(l, r, _) => bin("+", l, r)
    case Subtract(l, r, _) => bin("-", l, r)
    case Multiply(l, r, _) => bin("*", l, r)
    case Divide(l, r, _) => bin("/", l, r)
    case c: CheckOverflow => parse(c.child)
    case k: KnownNotNull => parse(k.child)
    case Coalesce(cs) =>
      val ps = cs.map(parse)
      if (ps.forall(_.isDefined)) Some(VCoalesce(ps.flatten)) else None
    case IsNull(c) => parse(c).map(VIsNull)
    case Substring(str, Literal(p, _), Literal(n, _)) =>
      for { c <- parse(str); pi <- asInt(p); ni <- asInt(n) } yield VSubstr(c, pi, ni)
    case Literal(null, _) => Some(VNull())
    case Literal(v, _) => lit(v)
    case _ => None
  }

  /** Binds attributes by ordinal in `out` (shuffle rows), not by name. */
  def parseAt(e: Expression, out: Seq[Attribute]): Option[VExpr] = e match {
    case a: Attribute =>
      val i = out.indexWhere(_.exprId == a.exprId)
      val j = if (i >= 0) i else out.indexWhere(_.name == a.name)
      if (j >= 0) Some(VCol("#" + j)) else None
    case Alias(c, _) => parseAt(c, out)
    case c: Cast => parseAt(c.child, out).map(ch => VCast(ch, c.dataType.simpleString))
    case u: UnscaledValue => parseAt(u.child, out).map(VUnscaled)
    case UnaryMinus(c, _) => parseAt(c, out).map(VNeg)
    case Add(l, r, _) => binAt("+", l, r, out)
    case Subtract(l, r, _) => binAt("-", l, r, out)
    case Multiply(l, r, _) => binAt("*", l, r, out)
    case Divide(l, r, _) => binAt("/", l, r, out)
    case c: CheckOverflow => parseAt(c.child, out)
    case k: KnownNotNull => parseAt(k.child, out)
    case Coalesce(cs) =>
      val ps = cs.map(c => parseAt(c, out))
      if (ps.forall(_.isDefined)) Some(VCoalesce(ps.flatten)) else None
    case IsNull(c) => parseAt(c, out).map(VIsNull)
    case Substring(str, Literal(p, _), Literal(n, _)) =>
      for { c <- parseAt(str, out); pi <- asInt(p); ni <- asInt(n) } yield VSubstr(c, pi, ni)
    case Literal(null, _) => Some(VNull())
    case Literal(v, _) => lit(v)
    case _ => None
  }

  def encode(e: VExpr): String = e match {
    case VCol(n) => "{" + n + "}"
    case VLong(v) => v.toString
    case VDouble(v) => v.toString
    case VStr(v) => "\"" + v.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
    case VNull() => "null"
    case VCast(c, t) => "(cast " + encode(c) + " " + encode(VStr(t)) + ")"
    case VUnscaled(c) => "(unscaled " + encode(c) + ")"
    case VNeg(c) => "(neg " + encode(c) + ")"
    case VBin(op, l, r) => "(" + op + " " + encode(l) + " " + encode(r) + ")"
    case VCoalesce(ks) => "(coalesce " + ks.map(encode).mkString(" ") + ")"
    case VIsNull(c) => "(isnull " + encode(c) + ")"
    case VSubstr(c, p, n) => "(substr " + encode(c) + " " + p + " " + n + ")"
  }

  def firstCol(e: VExpr): Option[String] = e match {
    case VCol(n) if !n.startsWith("#") => Some(n)
    case VCol(_) => None
    case VCast(c, _) => firstCol(c)
    case VUnscaled(c) => firstCol(c)
    case VNeg(c) => firstCol(c)
    case VBin(_, l, r) => firstCol(l).orElse(firstCol(r))
    case VCoalesce(ks) => ks.flatMap(firstCol).headOption
    case VIsNull(c) => firstCol(c)
    case VSubstr(c, _, _) => firstCol(c)
    case _ => None
  }

  /** Column names a file scan must read. Ordinals are not names. */
  def colNames(encoded: String): Seq[String] = {
    if (encoded == null || !encoded.contains("{")) {
      Nil
    } else {
      decode(encoded).toSeq.flatMap(colsOf)
    }
  }

  def evalEncoded(encoded: String, names: Array[String], row: Array[Any]): Any = {
    eval(decode(encoded).getOrElse(VNull()), names, row)
  }

  def eval(e: VExpr, names: Array[String], row: Array[Any]): Any = e match {
    case VCol(n) =>
      if (n.startsWith("#")) {
        val i = n.substring(1).toInt
        if (i >= 0 && i < row.length) row(i) else null
      } else {
        val i = names.indexOf(n)
        if (i >= 0 && i < row.length) row(i) else null
      }
    case VLong(v) => v
    case VDouble(v) => v
    case VStr(v) => v
    case VNull() => null
    case VIsNull(c) => eval(c, names, row) == null
    case VNeg(c) => num(eval(c, names, row)).map(v => -v).orNull
    case VUnscaled(c) => num(eval(c, names, row)).orNull
    case VCast(c, t) => cast(eval(c, names, row), t)
    case VBin(op, l, r) =>
      val a = num(eval(l, names, row))
      val b = num(eval(r, names, row))
      (a, b) match {
        case (Some(x), Some(y)) =>
          op match {
            case "+" => x + y
            case "-" => x - y
            case "*" => x * y
            case "/" => if (y == 0.0) null else x / y
            case _ => null
          }
        case _ => null
      }
    case VCoalesce(ks) =>
      ks.iterator.map(k => eval(k, names, row)).find(_ != null).orNull
    case VSubstr(c, pos, len) =>
      val s = eval(c, names, row)
      if (s == null || len <= 0) {
        null
      } else {
        val str = s.toString
        val start = if (pos > 0) pos - 1 else 0
        if (start >= str.length) "" else str.substring(start, math.min(str.length, start + len))
      }
  }

  def decode(s: String): Option[VExpr] = {
    try {
      val (e, rest) = parseTok(s.trim)
      if (rest.trim.isEmpty) Some(e) else None
    } catch {
      case _: RuntimeException => None
    }
  }

  private def colsOf(e: VExpr): Seq[String] = e match {
    case VCol(n) if !n.startsWith("#") => Seq(n)
    case VCast(c, _) => colsOf(c)
    case VUnscaled(c) => colsOf(c)
    case VNeg(c) => colsOf(c)
    case VBin(_, l, r) => colsOf(l) ++ colsOf(r)
    case VCoalesce(ks) => ks.flatMap(colsOf)
    case VIsNull(c) => colsOf(c)
    case VSubstr(c, _, _) => colsOf(c)
    case _ => Nil
  }

  private def bin(op: String, l: Expression, r: Expression): Option[VExpr] = {
    for { a <- parse(l); b <- parse(r) } yield VBin(op, a, b)
  }

  private def binAt(
      op: String,
      l: Expression,
      r: Expression,
      out: Seq[Attribute]): Option[VExpr] = {
    for { a <- parseAt(l, out); b <- parseAt(r, out) } yield VBin(op, a, b)
  }

  private def asInt(v: Any): Option[Int] = v match {
    case i: java.lang.Integer => Some(i.intValue())
    case l: java.lang.Long => Some(l.intValue())
    case i: Int => Some(i)
    case l: Long => Some(l.toInt)
    case _ => None
  }

  private def lit(v: Any): Option[VExpr] = v match {
    case i: java.lang.Integer => Some(VLong(i.longValue()))
    case l: java.lang.Long => Some(VLong(l.longValue()))
    case i: Int => Some(VLong(i.toLong))
    case l: Long => Some(VLong(l))
    case d: java.lang.Double => Some(VDouble(d.doubleValue()))
    case f: java.lang.Float => Some(VDouble(f.doubleValue()))
    case d: Double => Some(VDouble(d))
    case f: Float => Some(VDouble(f.toDouble))
    case dec: Decimal => Some(VDouble(dec.toDouble))
    case s: UTF8String => Some(VStr(s.toString))
    case s: String => Some(VStr(s))
    case b: java.lang.Boolean => Some(VLong(if (b) 1L else 0L))
    case _ => None
  }

  private def num(v: Any): Option[Double] = v match {
    case null => None
    case d: java.lang.Double => Some(d.doubleValue())
    case l: java.lang.Long => Some(l.doubleValue())
    case i: java.lang.Integer => Some(i.doubleValue())
    case f: java.lang.Float => Some(f.doubleValue())
    case b: java.lang.Boolean => Some(if (b) 1.0 else 0.0)
    case d: Double => Some(d)
    case l: Long => Some(l.toDouble)
    case i: Int => Some(i.toDouble)
    case dec: Decimal => Some(dec.toDouble)
    case s: String =>
      try {
        Some(s.toDouble)
      } catch {
        case _: NumberFormatException => None
      }
    case other =>
      try {
        Some(other.toString.toDouble)
      } catch {
        case _: NumberFormatException => None
      }
  }

  private def cast(v: Any, tpe: String): Any = {
    if (v == null) {
      null
    } else if (tpe == "string") {
      v.toString
    } else if (tpe.startsWith("decimal")) {
      num(v).orNull
    } else {
      num(v).orNull
    }
  }

  private def parseTok(s: String): (VExpr, String) = {
    if (s.startsWith("null")) {
      (VNull(), s.substring(4))
    } else if (s.startsWith("{")) {
      val end = s.indexOf('}')
      if (end < 0) throw new RuntimeException("col")
      (VCol(s.substring(1, end)), s.substring(end + 1))
    } else if (s.startsWith("\"")) {
      val (str, rest) = readStr(s)
      (VStr(str), rest)
    } else if (s.startsWith("(")) {
      parseList(s)
    } else {
      val tok = s.takeWhile(c => !c.isWhitespace && c != ')')
      val rest = s.substring(tok.length)
      if (tok.contains(".")) {
        (VDouble(tok.toDouble), rest)
      } else {
        (VLong(tok.toLong), rest)
      }
    }
  }

  private def parseList(s: String): (VExpr, String) = {
    var rest = s.substring(1).trim
    val (op, afterOp) = readWord(rest)
    rest = afterOp.trim
    val args = scala.collection.mutable.ArrayBuffer.empty[VExpr]
    while (!rest.startsWith(")")) {
      if (rest.isEmpty) throw new RuntimeException("list")
      val (e, more) = parseTok(rest)
      args += e
      rest = more.trim
    }
    val built = op match {
      case "+" | "-" | "*" | "/" if args.length == 2 => VBin(op, args(0), args(1))
      case "neg" if args.length == 1 => VNeg(args(0))
      case "unscaled" if args.length == 1 => VUnscaled(args(0))
      case "cast" if args.length == 2 =>
        args(1) match {
          case VStr(t) => VCast(args(0), t)
          case _ => throw new RuntimeException("cast")
        }
      case "isnull" if args.length == 1 => VIsNull(args(0))
      case "coalesce" if args.nonEmpty => VCoalesce(args.toSeq)
      case "substr" if args.length == 3 =>
        (args(1), args(2)) match {
          case (VLong(p), VLong(n)) => VSubstr(args(0), p.toInt, n.toInt)
          case _ => throw new RuntimeException("substr")
        }
      case _ => throw new RuntimeException(op)
    }
    (built, rest.substring(1))
  }

  private def readWord(s: String): (String, String) = {
    val n = s.takeWhile(c => !c.isWhitespace && c != ')').length
    (s.substring(0, n), s.substring(n))
  }

  private def readStr(s: String): (String, String) = {
    val b = new StringBuilder
    var i = 1
    while (i < s.length && s.charAt(i) != '"') {
      if (s.charAt(i) == '\\' && i + 1 < s.length) {
        b.append(s.charAt(i + 1))
        i += 2
      } else {
        b.append(s.charAt(i))
        i += 1
      }
    }
    if (i >= s.length) throw new RuntimeException("str")
    (b.toString, s.substring(i + 1))
  }
}
