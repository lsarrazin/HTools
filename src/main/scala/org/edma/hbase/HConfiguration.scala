// Copyright (C) 2017 Laurent Sarrazin & other authors
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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.hadoop.hbase.HBaseConfiguration

class HConfiguration {

  private def optCoreSite = "org.edma.hbase.core-site"
  private def optHBaseSite = "org.edma.hbase.hbase-site"
  private def optPrincipal = "org.edma.hbase.principal"
  private def optKeytab = "org.edma.hbase.keytab"

  /** Configuration for HBase connection & wrapper */
  private val hbconf: Configuration = HBaseConfiguration.create

  /** Default configuration */
  private val confDefaults: Map[String, (String, String)] = Map(
    "hs" -> (defaultHBaseSite, optHBaseSite),
    "cs" -> (defaultCoreSite, optCoreSite))

  def getPrincipal: String = hbconf.get(optPrincipal, "")
  def getKeytab: String = hbconf.get(optKeytab, "")
    
  def getConf: Configuration = {
    confDefaults.foreach {
      case (opt, (value, flag)) =>
        opt match {
          case "cs" => if (hbconf.get(flag, "") == "") {
            hbconf.addResource(new Path(value))
            hbconf.set(flag, value)
          }
          case "hs" => if (hbconf.get(flag, "") == "") {
            hbconf.addResource(new Path(value))
            hbconf.set(flag, value)
          }
        }
    }

    hbconf
  }

  def configure(opt: String, value: String): HConfiguration = {
    opt match {
      case "cs" => {
        hbconf.addResource(new Path(value))
        hbconf.set(optCoreSite, value)
      }
      case "hs" => {
        hbconf.addResource(new Path(value))
        hbconf.set(optHBaseSite, value)
      }
      case "zq" => hbconf.set("hbase.zookeeper.quorum", value)
      case "zp" => hbconf.set("hbase.zookeeper.property.clientPort", value)
      case "kc" => {
        hbconf.set("hadoop.security.authentication", "kerberos")
        hbconf.set("hbase.security.authentication", "kerberos")
      }

      case "p" => hbconf.set(optPrincipal, value)
      case "kt" => hbconf.set(optKeytab, value)

      case _ => // Do nothing
    }

    this
  }

  def dump: Unit = {
    def coreSite = hbconf.get(optCoreSite)
    def hbaseSite = hbconf.get(optHBaseSite)
    def principal = hbconf.get(optPrincipal)
    def keytab = hbconf.get(optKeytab)

    echo(f"""Configured using
             | - core-site.xml $coreSite
             | - hbase-site.xml $hbaseSite
             | - keytab $keytab
             | - principal $principal""".stripMargin)
  }

  def debug: Unit = {
    import scala.collection.JavaConverters._

    hbconf.asScala.foreach(dumpKV)

    def dumpKV(e: java.util.Map.Entry[String, String]) = {
      echo(f"${e.getKey} = ${e.getValue}")
    }
  }
}
