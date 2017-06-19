// Copyright (C) 2017 EDMA team & other authors
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.edma

import org.apache.hadoop.hbase.util.Bytes

package object hbase {
  /** Default Hadoop configuration */
  val defaultCoreSite = "/etc/hbase/conf/core-site.xml"
  val defaultHBaseSite = "/etc/hbase/conf/hbase-site.xml"

  /** Constants */
  val databases: String = "databases"
  val namespaces: String = "namespaces"
  val tables: String = "tables"
  
  /** Technical information within HBase */
  val schemaRowId: Array[Byte] = "$$EDMA$$"
  val schemaCfId: Array[Byte] = "d"
  val schemaCqId: Array[Byte] = "$$SCHEMA$$"

  /** I/O */
  def error(msg: String*): Unit = msg.foreach(error)
  def echo(msg: String*): Unit = msg.foreach(echo)
  def error(msg: String): Unit = println(f"[Error] $msg")
  def echo(msg: String): Unit = println(msg)
  
  implicit def stringToBytes(s: String): Array[Byte] = Bytes.toBytes(s)
  implicit def bytesToString(ab: Array[Byte]): String = Bytes.toString(ab)
}
