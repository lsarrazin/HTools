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

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.security.UserGroupInformation

import org.apache.hadoop.hbase.TableName

trait HConnection {

  /** Connect to HBase as default user (from configuration) */
  def connect(): HConnection

  /**
   * Connect to HBase as default user (from configuration)
   *  @param keytab Path to keytab
   *  @param principal Kerberos principal to use to HBase
   */
  def connect(keytab: String, principal: String): HConnection

  /** Reset connection */
  def reconnect(): HConnection

  /** Disconnect from HBase */
  def disconnect(): HConnection

  /** Get connection status */
  def status(): String

  /** Retrieve HBase table handler */
  def getTable(tname: String): HTable = new HInvalidTable(tname)

  /** Check if HBase table exists */
  def hasTable(tname: String): Boolean = {
    warn("Impossible to know of table " + tname + " without connection to HBase")
    false
  }

  /** List HBase tables from connection */
  def listTables: Array[String] = {
    warn("Can not get tables as not connected yet")
    Array.empty[String]
  }

}

class HConfiguredConnection(conf: HConfiguration) extends HConnection {

  /** Connect to HBase as default user (from configuration) */
  def connect: HConnection = {
    try {
      connect(conf.getKeytab, conf.getPrincipal)
    } catch {
      case nse: NoSuchElementException => {
        error("Kerberos information is missing")
        this
      }
    }
  }

  /**
   * Connect to HBase as default user (from configuration)
   * @param keytab Path to keytab
   * @param principal Kerberos principal to use to HBase
   */
  def connect(keytab: String, principal: String): HConnection = {
    val config = conf.getConf

    echo(f"Connecting to HBase as $principal", f"  using keytab $keytab")

    UserGroupInformation.setConfiguration(config)
    UserGroupInformation.loginUserFromKeytab(principal, keytab)

    val conn: Connection = ConnectionFactory.createConnection(config)
    if (conn.isClosed()) {
      error("Unable to get HBase connection")
      this
    } else {
      new HRunnableConnection(conn, conf)
    }
  }

  def reconnect(): HConnection = {
    error("Not connected, use connect first")
    this
  }
  
  def disconnect(): HConnection = {
    error("Not connected yet")
    this
  }

  /** Show connection status */
  def status(): String = {
    val keytab = conf.getKeytab
    val principal = conf.getPrincipal

    f"Configured for HBase with $principal using keytab $keytab"
  }

}

/** HRunnableConnection represents a connected state of the connection */
class HRunnableConnection(conn: Connection, conf: HConfiguration) extends HConfiguredConnection(conf) {

  override def connect: HConnection = {
    warn("Already connected")
    this
  }
  
  override def connect(keytab: String, principal: String): HConnection = {
    val ckeytab = conf.getKeytab
    val cprincipal = conf.getPrincipal
    
    if ((keytab == ckeytab) && (principal == cprincipal)) {
      warn("Already connected as " + principal)
      this
    } else {
      disconnect
      
      val newConf = conf.configure("p", principal).configure("kt", keytab)
      new HConfiguredConnection(conf).connect(keytab, principal)
    }
  }
  
  /**
   * Low-level HBase interaction, handling connection
   * @param defValue Value to return in case of error
   * @param hf Function to execute over existing hbase connection
   */
  def hCall[T](defValue: T)(hf: (Connection) => T): T = {
    hf(conn)
  }

  /**
   * Low-level HBase administration interaction, handling connection
   * @param defValue Value to return in case of error
   * @param fa Admin function to operate over existing hbase connection
   */
  def adminCall[T](defValue: T)(fa: (Admin) => T): T = {
    hCall(defValue)((conn: Connection) => {
      val adm: Admin = conn.getAdmin
      val res: T = fa(adm)
      adm.close()
      res
    })
  }

  /**
   * Disconnect from HBase
   */
  override def disconnect: HConnection = {
    conn.close
    new HConfiguredConnection(conf)
  }

  /** Reconnect to HBase */
  override def reconnect: HConnection = {
    disconnect.connect()
  }

  /** Show connection status */
  override def status: String = {
    val cs = super.status
    if (UserGroupInformation.isLoginTicketBased) {
      cs + "\n  logged using ticket"
    } else {
      cs
    }
  }

  /** Retrieve HBase table handler */
  override def getTable(tname: String): HTable =
    HTable(getHBaseTable(tname), tname)

  /** Retrieve HBase Table Handler */
  def getHBaseTable(tname: String): Table = conn.getTable(TableName.valueOf(tname))

  /** Check if HBase table exists */
  override def hasTable(tname: String): Boolean = {
    val hname = TableName.valueOf(tname)
    adminCall(false)(_.tableExists(hname))
  }
  
  /** List all tables from connection */
  override def listTables: Array[String] = {
    def listTables(admin: Admin): Array[String] = {
      // list the tables
      val listTables = admin.listTables()
      listTables.map(_.getNameAsString)
    }

    adminCall(Array.empty[String])(listTables)
  }
  
}

class HDummyConnection extends HConnection {

  /** Connect to HBase as default user (from configuration) */
  override def connect(): HConnection = {
    debug("Fake connection to HBase => success")
    new HSimulatedConnection
  }

  /**
   * Connect to HBase as default user (from configuration)
   *  @param keytab Path to keytab
   *  @param principal Kerberos principal to use to HBase
   */
  override def connect(keytab: String, principal: String): HConnection = {
    debug("Fake connection to HBase as " + principal + "=> success")
    new HSimulatedConnection
  }

  /** Reset connection */
  override def reconnect(): HConnection = {
    error("Dummy connection misses connection to reconnect to HBase")
    this
  }

  /** Disconnect from HBase */
  override def disconnect(): HConnection = {
    error("Dummy connection misses connection to disconnect from HBase")
    this
  }

  /** Get connection status */
  override def status(): String = "Dummy HBase Connection"

}

class HSimulatedConnection extends HDummyConnection {

  /** Connect to HBase as default user (from configuration) */
  override def connect(): HConnection = {
    debug("Fake connection already connected to HBase => ignored")
    this
  }

  /**
   * Connect to HBase as default user (from configuration)
   *  @param keytab Path to keytab
   *  @param principal Kerberos principal to use to HBase
   */
  override def connect(keytab: String, principal: String): HConnection = {
    debug("Fake connection already connected to HBase => ignored")
    this
  }

  /** Reset connection */
  override def reconnect(): HConnection = {
    debug("Dummy connection reconnected to HBase")
    this
  }

  /** Disconnect from HBase */
  override def disconnect(): HConnection = {
    debug("Dummy connection disconnecting from HBase")
    new HDummyConnection
  }

  /** Get connection status */
  override def status(): String = "Dummy & connected to HBase Connection"

  /** Check if HBase table exists */
  override def hasTable(tname: String): Boolean = {
    debug("Dummy connection simulates all tables")
    true
  }

  override def listTables: Array[String] = {
    warn("Dummy connection does not list tables")
    Array.empty[String]
  }

}

object HConnection {

  def configure(conf: HConfiguration): HConnection =
    new HConfiguredConnection(conf)

  def test: HConnection = new HDummyConnection

}
