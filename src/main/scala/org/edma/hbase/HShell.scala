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


abstract class HShell(conf: HConfiguration) {
  
  def getConnection: HConnection
  def getConfiguration: HConfiguration = conf
  
  /** Open connection using default configuration */
  def connect: HShell
  
  /** Close connection */
  def disconnect: HShell
  
  /** Reset connection */
  def reconnect: HShell
  
  /** Enquire connection status */
  def status: String
  
  /**
   * Low-level table list
   * @return Array of accessible (granted) tables
   */
  def list: Array[String]
  
  def listNamespaces: Array[String] = list.map(_.split(":")(0))
  def listTables(ns: String): Array[String] = list.filter(_.startsWith(ns)).map(_.split(":")(1))
  def listTables: Array[String] = list.map(_.split(":")(1))

  def showTables(ns: String): Unit = listTables(ns).foreach(echo)
  def showTables(): Unit = listTables.foreach(echo)
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
  
  /** Get current namespace */
  def getUseNS: String
  
  /** Set current namespace
   *  @param ns namespace to use as default */
  def use(ns: String): HShell
  
  /** Normalize table name
   *  @param tname Table name to normalize */
  def normalizeTableName(tname: String): String = {
    if (tname.contains(":")) {
      tname
    } else {
      val defNS = getUseNS
      if (defNS.length > 0)
        defNS + ":" + tname
      else 
        tname
    }
  }

  /** Describe table using schema */
  def desc(tname: String): Unit
}


class HDisconnectedShell(conf: HConfiguration) extends HShell(conf) {
  
  val conn: HConnection = HConnection.configure(conf)
  def getConnection: HConnection = conn
  
  override def connect: HShell = {
    echo("Connecting to HBase")
    val cconn = conn.connect
    if (cconn.isConnected) {
      new HConnectedShell(conf, cconn)
    } else {
      new HDisconnectedShell(conf)
    }
  }

  override def disconnect: HShell = {
    error("Not connected to HBase")
    this
  }
  
  override def reconnect: HShell = {
    warn("Not connected to HBase")
    connect
  }
  
  override def status: String = "Not connected to HBase"
  
  override def list: Array[String] = Array[String]()

  def getUseNS: String = ""
  
  def use(ns: String): HShell = this
  
  override def desc(tname: String): Unit = {
    error("Not connected to HBase")
  }
}


class HConnectedShell(conf: HConfiguration, conn: HConnection, defNS: String = "") extends HShell(conf) {

  def getConnection: HConnection = conn

  override def connect: HShell = {
    error("Already connected to HBase")
    this
  }

  override def disconnect: HShell = {
    echo("Disconnecting from HBase")
    conn.disconnect
    new HDisconnectedShell(conf)
  }
  
  override def reconnect: HShell = {
    conn.disconnect
    new HDisconnectedShell(conf).connect
  }

  override def list: Array[String] = conn.listTables
  
  override def getUseNS: String = defNS

  override def use(ns: String): HShell =
    if (listNamespaces.contains(ns)) {
      new HConnectedShell(conf, conn, ns)
    } else {
      error(f"NameSpace $ns does not exist")
      this
    }

  override def status: String = conn.status
  
  override def desc(tname: String): Unit = {
    val nname = normalizeTableName(tname)
    echo(f"Querying $nname for schema")
    
    val table: HTable = HTable(conn, nname)
    val schema: HSchema = new HSchema(table)
    
    val json = schema.toPrintable
    echo(json)
  }
}

object HShell extends HBaseToolsCommandProvider {

  private var current: Option[HShell] = None

  def create(conf: HConfiguration): HShell = {
    val sh = new HDisconnectedShell(conf)
    current = Some(sh)
    sh
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
    HBaseToolsCommand("desc", "def desc(table: String): Unit = { hsh.desc(table) }", "", "Describe table", "Get details about table"),
    HBaseToolsCommand("test", "def test: Unit = { echo(hsh.toString); echo(hsh.status) }", "", "Test command", "Give some details"),
    HBaseToolsCommand("connect", "def connect: Unit = { hsh = hsh.connect; echo(hsh.status); }", "", "Connect to HBase", "Connect to HBase using command line parameters"),
    HBaseToolsCommand("disconnect", "def disconnect: Unit = { hsh = hsh.disconnect; echo(hsh.status) }", "", "Disconnect from HBase", "Disconnect from HBase"),
    HBaseToolsCommand("status", "def status: Unit = { echo(hsh.status) }", "Show connection status", "", "Get details about current connection")
  )
}
