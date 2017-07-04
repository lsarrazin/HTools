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

class HConnection(conf: HConfiguration) {

  private var connected: Boolean = false
  private var connection: Option[Connection] = None

  /**
   * Low-level HBase interaction, handling connection
   * @param defValue Value to return in case of error
   * @param hf Function to execute over existing hbase connection
   */
  def hCall[T](defValue: T)(hf: (Connection) => T): T = {
    if (!connected) {
      error("Not connected")
      defValue
    } else {
      if (connection.isDefined) {
        hf(connection.get)
      } else {
        error("Internal error, connected without connection")
        defValue
      }
    }
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
   * Connect to HBase as default user (from configuration)
   */
  def connect: Unit = {
    if (connected) {
      echo("Already connected")
    } else {
      try {
        connect(conf.getKeytab, conf.getPrincipal)
      } catch {
        case nse: NoSuchElementException => error("Kerberos information is missing")
      }

    }
  }

  /**
   * Connect to HBase as default user (from configuration)
   * @param keytab Path to keytab
   * @param principal Kerberos principal to use to HBase
   */
  def connect(keytab: String, principal: String): Connection = {
    val config = conf.getConf

    echo(f"Connecting to HBase as $principal", f"  using keytab $keytab")

    UserGroupInformation.setConfiguration(config)
    UserGroupInformation.loginUserFromKeytab(principal, keytab)

    connection = Some(ConnectionFactory.createConnection(config))
    if (connection.get.isClosed()) {
      error("Unable to get HBase connection")
    } else {
      connected = true
    }
    connection.get
  }

  /**
   * Disconnect from HBase
   */
  def disconnect: Unit = {
    if (connected) {
      echo("Disconnected")
      connection.get.close
      connected = false
    } else {
      error("Not connected")
    }
  }

  /** Reconnect to HBase */
  def reconnect: Unit = {
    if (connected) disconnect
    connect
  }
  
  /** Show connection status */
  def status: Unit = {
    if (connected) {
      val keytab = conf.getKeytab
      val principal = conf.getPrincipal

      echo(f"Connected to HBase as $principal")
      echo(f"  using keytab $keytab")

      if (UserGroupInformation.isLoginTicketBased) {
        echo("  logged using ticket")
      }

    } else {
      echo("Not connected")
    }
  }

  /** Retrieve HBase table handler */
  def getTable(tname: TableName): Table = connection.get.getTable(tname)

  /** Check if HBase table exists */
  def hasTable(tname: TableName): Boolean = adminCall(false)(_.tableExists(tname))
}

object HConnection {
  
}
