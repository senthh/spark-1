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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.spark.sql.types.DataType

/**
 * Stable binary encoding of [[NativePlan]] for the JNI task API.
 * This is not Substrait. Version 2 adds StagePlan and richer HashAgg.
 * Version 3 adds ExpandSpec (rollup / grouping sets) on StagePlan.
 */
object NativePlanCodec {
  private val VERSION: Int = 3
  private val KIND_COUNT: Int = 1
  private val KIND_HASHAGG: Int = 2
  private val KIND_STAGE: Int = 3

  def encode(plan: NativePlan): Array[Byte] = {
    val buf = new ByteArrayOutputStream()
    val out = new DataOutputStream(buf)
    out.writeInt(VERSION)
    plan match {
      case CountStar(files) =>
        out.writeInt(KIND_COUNT)
        writeStrings(out, files)
      case h: HashAgg =>
        out.writeInt(KIND_HASHAGG)
        writeStrings(out, h.files)
        writeString(out, h.groupCol)
        writeString(out, h.sumCol)
        writeFilters(out, h.allFilters)
        out.writeInt(h.sumScale)
        writeString(out, h.groupType.json)
        writeString(out, h.sumType.json)
        out.writeBoolean(h.complete)
        writeStrings(out, h.groups)
        writeAggs(out, h.aggCalls)
        writeFileRefs(out, h.refs)
      case s: StagePlan =>
        out.writeInt(KIND_STAGE)
        writeScan(out, s.probe)
        out.writeInt(s.builds.length)
        s.builds.foreach { b =>
          writeScan(out, b.scan)
          writeStrings(out, b.probeKeys)
          writeStrings(out, b.buildKeys)
          out.writeInt(b.joinType)
          writeFilters(out, b.filters)
        }
        writeFilters(out, s.probeFilters)
        writeStrings(out, s.groups)
        writeStrings(out, s.groupTypes.map(_.json))
        writeAggs(out, s.aggs)
        s.window match {
          case Some(w) =>
            out.writeBoolean(true)
            writeStrings(out, w.partitionBy)
            out.writeInt(w.orderBy.length)
            w.orderBy.foreach { case (c, asc) =>
              writeString(out, c)
              out.writeBoolean(asc)
            }
            out.writeInt(w.fns.length)
            w.fns.foreach { f =>
              out.writeInt(f.kind)
              writeString(out, f.col)
              writeString(out, f.alias)
            }
          case None =>
            out.writeBoolean(false)
        }
        out.writeBoolean(s.complete)
        writeExpand(out, s.expand)
    }
    out.flush()
    buf.toByteArray
  }

  def decode(bytes: Array[Byte]): NativePlan = {
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    val version = in.readInt()
    if (version < 1 || version > 3) {
      throw new IllegalArgumentException(s"unsupported NativePlan version $version")
    }
    in.readInt() match {
      case KIND_COUNT =>
        CountStar(readStrings(in))
      case KIND_HASHAGG =>
        decodeHashAgg(in, version)
      case KIND_STAGE =>
        decodeStage(in, version)
      case other =>
        throw new IllegalArgumentException(s"unsupported NativePlan kind $other")
    }
  }

  private def decodeHashAgg(in: DataInputStream, version: Int): HashAgg = {
    val files = readStrings(in)
    val groupCol = readString(in)
    val sumCol = readString(in)
    val filters = if (version >= 2) {
      readFilters(in)
    } else {
      if (in.readBoolean()) {
        Seq(FilterPred(readString(in), in.readLong(), in.readInt()))
      } else {
        Nil
      }
    }
    val sumScale = in.readInt()
    val groupType = DataType.fromJson(readString(in))
    val sumType = DataType.fromJson(readString(in))
    val complete = in.readBoolean()
    if (version < 2) {
      HashAgg(
        files, groupCol, sumCol, filters.headOption, sumScale, groupType, sumType, complete)
    } else {
      val groupCols = readStrings(in)
      val aggs = readAggs(in)
      val refs = readFileRefs(in)
      HashAgg(
        files, groupCol, sumCol, filters.headOption, sumScale, groupType, sumType, complete,
        groupCols, aggs, filters, refs)
    }
  }

  private def decodeStage(in: DataInputStream, version: Int): StagePlan = {
    val probe = readScan(in)
    val n = in.readInt()
    val builds = Seq.fill(n) {
      BuildJoin(
        scan = readScan(in),
        probeKeys = readStrings(in),
        buildKeys = readStrings(in),
        joinType = in.readInt(),
        filters = readFilters(in))
    }
    val probeFilters = readFilters(in)
    val groups = readStrings(in)
    val groupTypes = readStrings(in).map(DataType.fromJson)
    val aggs = readAggs(in)
    val window = if (in.readBoolean()) {
      val part = readStrings(in)
      val on = in.readInt()
      val order = Seq.fill(on)((readString(in), in.readBoolean()))
      val fn = in.readInt()
      val fns = Seq.fill(fn)(WindowCall(in.readInt(), readString(in), readString(in)))
      Some(WinSpec(part, order, fns))
    } else {
      None
    }
    val complete = in.readBoolean()
    val expand = if (version >= 3) readExpand(in) else None
    StagePlan(probe, builds, probeFilters, groups, groupTypes, aggs, window, complete, expand)
  }

  private def writeScan(out: DataOutputStream, s: ScanSpec): Unit = {
    writeFileRefs(out, s.files)
    writeStrings(out, s.columns)
  }

  private def readScan(in: DataInputStream): ScanSpec = {
    ScanSpec(readFileRefs(in), readStrings(in))
  }

  private def writeFileRefs(out: DataOutputStream, refs: Seq[FileRef]): Unit = {
    out.writeInt(refs.length)
    refs.foreach { r =>
      writeString(out, r.path)
      out.writeInt(r.parts.length)
      r.parts.foreach { case (k, v) =>
        writeString(out, k)
        writeString(out, v)
      }
    }
  }

  private def readFileRefs(in: DataInputStream): Seq[FileRef] = {
    val n = in.readInt()
    Seq.fill(n) {
      val path = readString(in)
      val pn = in.readInt()
      val parts = Seq.fill(pn)((readString(in), readString(in)))
      FileRef(path, parts)
    }
  }

  private def writeFilters(out: DataOutputStream, fs: Seq[FilterPred]): Unit = {
    out.writeInt(fs.length)
    fs.foreach { f =>
      writeString(out, f.col)
      out.writeLong(f.value)
      out.writeInt(f.op)
      writeString(out, Option(f.strValue).getOrElse(""))
      out.writeDouble(f.dvalue)
    }
  }

  private def readFilters(in: DataInputStream): Seq[FilterPred] = {
    val n = in.readInt()
    Seq.fill(n) {
      FilterPred(readString(in), in.readLong(), in.readInt(), readString(in), in.readDouble())
    }
  }

  private def writeAggs(out: DataOutputStream, aggs: Seq[AggCall]): Unit = {
    out.writeInt(aggs.length)
    aggs.foreach { a =>
      out.writeInt(a.kind)
      writeString(out, a.col)
      out.writeInt(a.scale)
      writeString(out, a.dataType.json)
    }
  }

  private def readAggs(in: DataInputStream): Seq[AggCall] = {
    val n = in.readInt()
    Seq.fill(n) {
      AggCall(in.readInt(), readString(in), in.readInt(), DataType.fromJson(readString(in)))
    }
  }

  private def writeExpand(out: DataOutputStream, expand: Option[ExpandSpec]): Unit = {
    expand match {
      case Some(e) =>
        out.writeBoolean(true)
        writeStrings(out, e.outCols)
        out.writeInt(e.projections.length)
        e.projections.foreach { row =>
          out.writeInt(row.length)
          row.foreach { s =>
            out.writeInt(s.kind)
            writeString(out, s.col)
            out.writeLong(s.lvalue)
            out.writeDouble(s.dvalue)
            writeString(out, s.svalue)
          }
        }
      case None =>
        out.writeBoolean(false)
    }
  }

  private def readExpand(in: DataInputStream): Option[ExpandSpec] = {
    if (!in.readBoolean()) {
      None
    } else {
      val names = readStrings(in)
      val n = in.readInt()
      val projs = Seq.fill(n) {
        val w = in.readInt()
        Seq.fill(w) {
          ExpandSlot(in.readInt(), readString(in), in.readLong(), in.readDouble(), readString(in))
        }
      }
      Some(ExpandSpec(names, projs))
    }
  }

  private def writeString(out: DataOutputStream, s: String): Unit = {
    val bytes = s.getBytes("UTF-8")
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  private def writeStrings(out: DataOutputStream, ss: Seq[String]): Unit = {
    out.writeInt(ss.length)
    ss.foreach(writeString(out, _))
  }

  private def readString(in: DataInputStream): String = {
    val n = in.readInt()
    val bytes = new Array[Byte](n)
    in.readFully(bytes)
    new String(bytes, "UTF-8")
  }

  private def readStrings(in: DataInputStream): Seq[String] = {
    val n = in.readInt()
    Seq.fill(n)(readString(in))
  }
}
