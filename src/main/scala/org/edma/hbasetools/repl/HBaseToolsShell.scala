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

package org.edma.hbasetools.repl

import scala.reflect.io.File
import scala.tools.nsc.util.ClassPath
import scala.tools.nsc.{ GenericRunnerCommand, MainGenericRunner }

import org.apache.commons.cli.{ Options, CommandLine, CommandLineParser, BasicParser, HelpFormatter }
import org.edma.hbase.HShell

trait HBTShell extends MainGenericRunner {

  // Command line job options
  val options = (new Options)
    .addOption("h", "help", false, "Show this help")
    .addOption("t", "table-name", true, "Hbase table name")
    .addOption("hs", "hbase-site", true, "hbase-site.xml file path")
    .addOption("cs", "core-site", true, "hadoop core-site.xml file path")
    .addOption("zp", "zk-port", true, "zookeeper quorum port (default: 2181)")
    .addOption("zq", "zk-quorum", true, "zookeeper quorum (ex: host1,host2,host3)")
    .addOption("kc", "krb5conf", true, "krb5.conf file path")
    .addOption("p", "principal", true, "kerberos principal to use")
    .addOption("kt", "keytab", true, "kerberos principal's keytab to use")

  // Process command line options
  private def parseArgs(args: Array[String]): CommandLine = {
    val parser: CommandLineParser = new BasicParser
    val cmd: CommandLine = parser.parse(options, args)
    if (cmd.hasOption("h")) {
      val f: HelpFormatter = new HelpFormatter
      f.printHelp("Usage", options)
      System.exit(1)
    }
    cmd
  }

  /**
   * The main entry point for executing the REPL.
   *
   * This method is lifted from [[scala.tools.nsc.MainGenericRunner]] and modified to allow
   * for custom functionality, including determining at runtime if the REPL is running,
   * and making custom REPL colon-commands available to the user.
   *
   * @param args passed from the command line
   * @return `true` if execution was successful, `false` otherwise
   */
  override def process(args: Array[String]): Boolean = {

    // Parse options
    val opts = parseArgs(args)

    // Process command line arguments into a settings object, and use that to start the REPL.
    // We ignore params we don't care about - hence error function is empty
    val command = new GenericRunnerCommand(args.toList, _ => ())

    // Force the repl to be synchronous, so all cmds are executed in the same thread
    command.settings.Yreplsync.value = true

    // Load each class declared from the command line arguments
    def classLoaderURLs(cl: ClassLoader): Array[java.net.URL] = cl match {
      case null                       => Array()
      case u: java.net.URLClassLoader => u.getURLs ++ classLoaderURLs(cl.getParent)
      case _                          => classLoaderURLs(cl.getParent)
    }

    classLoaderURLs(Thread.currentThread().getContextClassLoader)
      .foreach(u => command.settings.classpath.append(u.getPath))

    // Useful settings for debugging, dumping class files etc:
    // command.settings.debug.value = true
    // command.settings.Yreploutdir.tryToSet(List(""))
    // command.settings.Ydumpclasses.tryToSet(List(""))

    /*      
    val replClassLoader = new ReplClassLoader(
      command.settings.classpathURLs.toArray ++
        classLoaderURLs(Thread.currentThread().getContextClassLoader),
      null,
      Thread.currentThread.getContextClassLoader)
      * *
      */

    val repl = new HBaseToolsILoop(opts)
    // replClassLoader.setRepl(repl)

    // Set classloader chain - expose top level abstract class loader down
    // the chain to allow for readObject and latestUserDefinedLoader
    // See https://gist.github.com/harrah/404272
    // command.settings.embeddedDefaults(replClassLoader)

    val res = repl.process(command.settings)

    // Terminate any HBase connection
    HShell.terminate

    res
  }

}

object HBaseToolsShell extends HBTShell {

  /** Runs an instance of the shell. */
  def main(args: Array[String]): Unit = {
    val retVal = process(args)
    if (!retVal) {
      sys.exit(1)
    }
  }
}
