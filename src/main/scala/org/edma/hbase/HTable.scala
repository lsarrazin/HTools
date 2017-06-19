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

trait HTableFunctions {

  val tname: String

  /** Table namespace from table name */
  def getTableNamespace: String =
    if (tname.contains(':')) { tname.split(':')(0) } else { "" }

  /** Raw table name (without namespace) */
  def getTableName: String =
    if (tname.contains(':')) { tname.split(':')(1) } else { tname }
  
  def hquery(conn: HConnection)(rowId: Get) = {
    
  }
}

class HTable(conn: HConnection, name: String) extends HTableFunctions {

  val tname: String = name

  /** Table as HBase Name */
  lazy val tableName: TableName = TableName.valueOf(tname)

  /** HBase table descriptor */
  lazy val tableDescriptor: HTableDescriptor = new HTableDescriptor(tableName)

  /** HBase table handler */
  lazy val table: Table = conn.getTable(tableName)
  
  /** Single value read */
  def get(key: Array[Byte], cf: Array[Byte], cq: Array[Byte]): Array[Byte] = {
    val get: Get = new Get(key).addColumn(cf, cq)
    val result: Result = table.get(get)
    result.value
  }

  def get(key: Array[Byte], cf: Array[Byte]): Result = {
    val get: Get = new Get(key).addFamily(cf)
    table.get(get)
  }

  /** Single value write */
  def put(key: Array[Byte], cf: Array[Byte], cq: Array[Byte], ts: Long = -1, value: Array[Byte]) = {
    val put: Put = if (ts < 0) {
      new Put(key).addColumn(cf, cq, value)
    } else {
      new Put(key).addColumn(cf, cq, ts, value)
    }
    table.put(put)
  }
}

object HTestTable extends HTableFunctions {

  val tname: String = "test:test"

}

