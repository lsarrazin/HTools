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

package org.edma.hbase

import org.apache.hadoop.hbase.client.{ HTable => HBaseTable, _ }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ CellUtil, HBaseConfiguration, HTableDescriptor, TableName }
import scala.collection.JavaConverters._

trait HTable {

  val tname: String

  /** Table namespace from table name */
  def getTableNamespace: String =
    if (tname.contains(':')) { tname.split(':')(0) } else { "" }

  /** Raw table name (without namespace) */
  def getTableName: String =
    if (tname.contains(':')) { tname.split(':')(1) } else { tname }

  def hquery(conn: HConnection)(rowId: Get) = {

  }

  /** Single value read */
  def get(key: Array[Byte], cf: Array[Byte], cq: Array[Byte]): Array[Byte]

  /** Single CF read */
  def get(key: Array[Byte], cf: Array[Byte]): Result

  /** Single value write */
  def put(key: Array[Byte], cf: Array[Byte], cq: Array[Byte], ts: Long = -1, value: Array[Byte]): Unit

}

class HValidTable(table: Table, name: String) extends HTable {

  val tname: String = name

  /** Table as HBase Name */
  lazy val tableName: TableName = TableName.valueOf(tname)

  /** HBase table descriptor */
  lazy val tableDescriptor: HTableDescriptor = new HTableDescriptor(tableName)

  /** Single value read */
  override def get(key: Array[Byte], cf: Array[Byte], cq: Array[Byte]): Array[Byte] = {
    val get: Get = new Get(key).addColumn(cf, cq)
    val result: Result = table.get(get)
    result.value
  }

  /** Single cf read */
  override def get(key: Array[Byte], cf: Array[Byte]): Result = {
    val get: Get = new Get(key).addFamily(cf)
    table.get(get)
  }

  /** Single value write */
  override def put(key: Array[Byte], cf: Array[Byte], cq: Array[Byte], ts: Long = -1, value: Array[Byte]) = {
    val put: Put = if (ts < 0) {
      new Put(key).addColumn(cf, cq, value)
    } else {
      new Put(key).addColumn(cf, cq, ts, value)
    }
    table.put(put)
  }
}

class HInvalidTable(name: String) extends HTable {

  val tname: String = name

  /** Single value read */
  override def get(key: Array[Byte], cf: Array[Byte], cq: Array[Byte]): Array[Byte] = {
    error("Read attempt on inexisting table " + tname)
    new Array[Byte](0)
  }

  override def get(key: Array[Byte], cf: Array[Byte]): Result = {
    error("Read attempt on inexisting table " + tname)
    new Result
  }

  /** Single value write */
  override def put(key: Array[Byte], cf: Array[Byte], cq: Array[Byte], ts: Long = -1, value: Array[Byte]) = {
    error("Write attempt on inexisting table " + tname)
  }

}

class HDummyTable(name: String) extends HTable {
  
  //TODO: append mutable hash map to simulate read/writes
  //TODO: declare a default dataset
  
  val tname: String = name

  /** Single value read */
  override def get(key: Array[Byte], cf: Array[Byte], cq: Array[Byte]): Array[Byte] = {
    debug("Read attempt on dummy table " + tname)
    new Array[Byte](0)
  }

  override def get(key: Array[Byte], cf: Array[Byte]): Result = {
    debug("Read attempt on dummy table " + tname)
    new Result
  }

  /** Single value write */
  override def put(key: Array[Byte], cf: Array[Byte], cq: Array[Byte], ts: Long = -1, value: Array[Byte]) = {
    debug("Write attempt on dummy table " + tname)
  }

}

object HTable {

  def apply(conn: HConnection, name: String): HTable = conn match {
    case rc: HRunnableConnection =>
      if (conn.hasTable(name)) {
        new HValidTable(rc.getHBaseTable(name), name)
      } else {
        new HInvalidTable(name)
      }
    case rc: HSimulatedConnection =>
      new HDummyTable(name)
    case _ => new HInvalidTable(name)
  }

  def apply(table: Table, name: String): HTable = new HValidTable(table, name)
}

