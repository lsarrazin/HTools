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

import org.apache.commons.cli.{ Options, CommandLine }

import org.apache.hadoop.hbase.{ HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName }
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.security.UserGroupInformation

import org.edma.hbasetools.repl.{HBaseToolsCommand, HBaseToolsCommandProvider}

class HShell(conf: HConfiguration) {

  lazy val conn: HConnection = new HConnection(conf)

  def disconnect: Unit = conn.disconnect
  def reconnect: Unit = conn.reconnect
  def connect: Unit = conn.connect

  /**
   * Low-level table list
   * @return Array of accessible (granted) tables
   */
  lazy val list: Array[String] = {
    def listTables(admin: Admin): Array[String] = {
      // list the tables
      val listTables = admin.listTables()
      listTables.map(_.getNameAsString)
    }

    conn.adminCall(Array.empty[String])(listTables)
  }

  def listNamespaces: Array[String] = list.map(_.split(":")(0))
  def listTables(ns: String): Array[String] = list.filter(_.startsWith(ns)).map(_.split(":")(1))
  def listTables: Array[String] = listTables("")

  def showTables: Unit = listTables(defaultNS).foreach(echo)
  def showNamespaces: Unit = listNamespaces.foreach(echo)

  /**
   * show command
   * @param what name of object to show (databases, tables)
   */
  def show(what: String): Unit = what.toLowerCase match {
    case `tables`     => showTables
    case `databases`  => showNamespaces
    case `namespaces` => showNamespaces
    case _            => error(f"Unknown object $what in show command")
  }
  
  /**
   * use command
   * @param ns namespace to use as default
   */
  def use(ns: String): Unit =
    if (listNamespaces.contains(ns)) {
      defaultNS = ns
    } else {
      error(f"NameSpace $ns does not exist")
    }

  def use: Unit = defaultNS = ""

  private var defaultNS: String = ""

  def normalizeTableName(tname: String): String = {
    if (tname.contains(":")) {
      tname
    } else {
      defaultNS + ":" + tname
    }
  }

  def status: Unit = conn.status
  
  def desc(tname: String): Unit = {
    val nname = normalizeTableName(tname)
    echo(f"Querying $nname for schema")
    
    val table: HTable = new HTable(conn, nname)
    val schema: HSchema = new HSchema(table)
    echo(schema.toJSon)
  }
}

object HShell extends HBaseToolsCommandProvider {

  private var current: Option[HShell] = None

  def create(conf: HConfiguration): HShell = {
    val conn = new HShell(conf)
    current = Some(conn)
    conn
  }

  def terminate: Unit =
    if (current.isDefined) current.get.disconnect

  def connect: Unit = if (current.isDefined) current.get.connect
  def disconnect: Unit = if (current.isDefined) current.get.disconnect

  def show: String => Unit =
    _ => if (current.isDefined) {
      current.get.show _
    }
 
  def getShellCommands: List[HBaseToolsCommand] = List(
    HBaseToolsCommand("connect", "", "def connect: Unit = hsh.connect", "Connect to HBase", "Connect to HBase using command line parameters"),
    HBaseToolsCommand("disconnect", "", "def disconnect: Unit = hsh.disconnect", "Disconnect from HBase", "Disconnect from HBase"),
    HBaseToolsCommand("status", "", "def status: Unit = hsh.status", "Show connection status", "Get details about current connection")
  )
}
