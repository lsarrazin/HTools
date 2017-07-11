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

import java.io.{ BufferedReader, StringReader, OutputStreamWriter }
import java.text.SimpleDateFormat

import scala.Predef.{println => _, _}
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{ILoop, JPrintWriter}
import scala.tools.nsc.util.stringFromStream
import scala.util.Properties.{javaVersion, javaVmName, versionString}

import org.apache.commons.cli.CommandLine

import org.edma.hbase.HShell

/**
 *  A Spark-specific interactive shell.
 */
class HBaseToolsILoop(hbaseOpts: CommandLine, in0: Option[BufferedReader], out: JPrintWriter)
    extends ILoop(in0, out) {

  def this(hbaseOpts: CommandLine, in0: BufferedReader, out: JPrintWriter) = this(hbaseOpts, Some(in0), out)
  def this(hbaseOpts: CommandLine) = this(hbaseOpts, None, new JPrintWriter(Console.out, true))

  lazy val commandProviders: List[HBaseToolsCommandProvider] = List(HShell)

  def initializeShell: Unit = {

    intp.beQuietDuring {
      processLine("""
        @transient val hconf0: org.edma.hbase.HConfiguration = new org.edma.hbase.HConfiguration
        println("Hbase configuration available as 'hconf0'")
        """)
      hbaseOpts.getOptions foreach { opt =>
        val p: String = opt.getOpt
        if (hbaseOpts.hasOption(p)) {
          val v: String = hbaseOpts.getOptionValue(p)
          processLine(f"""hconf0.configure("${p}", "${v}")""")
        }
      }
      processLine("""
        @transient var hsh: org.edma.hbase.HShell = org.edma.hbase.HShell.create(hconf0)
        println("Hbase shell available as 'hsh'")
        """)

    }

    processLine("import org.edma.hbase.HShell._")
    processLine("import org.edma.hbase._")

    commandProviders foreach declareCommands

    replayCommandStack = Nil // remove above commands from session history.
  }

  def declareCommands(hbcp: HBaseToolsCommandProvider): Unit = {
    intp.beQuietDuring {
      hbcp.getShellCommands foreach {
        cmd: HBaseToolsCommand => processLine(cmd.getCode)
      }
    }
  }

  def listCommands(hbcp: HBaseToolsCommandProvider): Unit = {
    hbcp.getShellCommands foreach {
      cmd: HBaseToolsCommand => echo(cmd.getLine)
    }
  }

  /** Print a welcome message */
  override def printWelcome(): Unit = {
    val ascii = """______ _____          ______
                  |   / /___/ /___________  / /____
                  |  / __ \  __/ __ \/ __ \/ / ___/
                  | / / / / /_/ /_/ / /_/ / (__  )
                  |/_/ /_/\__/\____/\____/_/____/""".stripMargin + "   version " + hbtVersion + "\n"
    echo(ascii)

    echo("Using Scala version %s (%s, Java %s)".format(versionString, javaVmName, javaVersion))
    echo("Type in expressions to have them evaluated or :help for more information.")
    echo("")
    echo("Defined hconf0 for configuration & hsh for hbase shell")
    commandProviders foreach listCommands
  }

  override def prompt: String = /* Console.GREEN + */ "\nhtools> " /* + Console.RESET */

  /** Add repl commands that needs to be blocked. e.g. reset */
  private val blockedCommands = Set[String]()

  /** Standard commands */
  lazy val shellStandardCommands: List[HBaseToolsILoop.this.LoopCommand] =
    standardCommands.filter(cmd => !blockedCommands(cmd.name))

  /** Available commands */
  override def commands: List[LoopCommand] = shellStandardCommands ++ additionalCommands

  /**
   * We override `loadFiles` because we need to initialize the shell *before* the REPL
   * sees any files, so that the HBase shell is visible in those files. This is a bit of a
   * hack, but there isn't another hook available to us at this point.
   */
  override def loadFiles(settings: Settings): Unit = {
    initializeShell
    super.loadFiles(settings)
  }

  /** Reset commands */
  override def resetCommand(line: String): Unit = {
    super.resetCommand(line)
    initializeShell
    echo("Note that after :reset, state of 'hsh' and 'hconf0' is unchanged.")
  }

  /** Convert EPOCH to Time */
  private def epochToTimeCmdImpl(es: String) = {

    def timeToStr(epochMillis: Long): String =
      new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(epochMillis)

    echoCommandMessage(timeToStr(es.toLong))
    Result.default
  }

  val epochToTimeCmd: this.LoopCommand = LoopCommand.cmd("e2t", "<epoch value>", "convert from EPOCH to date literal", epochToTimeCmdImpl)

  /** Convert time string to EPOCH */
  private def timeToEpochCmdImpl(ts: String) = {

    val epoch: Long = if (ts == "now") {
      System.currentTimeMillis
    } else {
      0
    }
    echoCommandMessage(epoch.toString)
    Result.default
  }

  val timeToEpochCmd: this.LoopCommand = LoopCommand.cmd("t2e", "<now | date_literal>", "convert from date literal to EPOCH", timeToEpochCmdImpl)

  private def statusCmdImpl(name: String) = {
    val hb = if (name.nonEmpty) name else "hsh"
    val res = intp.interpret(s"$hb.status")
    Result.default
  }

  val statusCmd: this.LoopCommand = LoopCommand.cmd("status", "<[connection] | hbase>", "read HBase wrapper status", statusCmdImpl)

  /** Additional commands */
  def additionalCommands: List[this.LoopCommand] = List(statusCmd, epochToTimeCmd, timeToEpochCmd)

}

object HBaseToolsILoop {

  /**
   * Creates an interpreter loop with default settings and feeds
   * the given code to it as input.
   */
  def run(hbaseOpts: CommandLine, code: String, sets: Settings = new Settings): String = {

    stringFromStream { ostream =>
      Console.withOut(ostream) {
        val input = new BufferedReader(new StringReader(code))
        val output = new JPrintWriter(new OutputStreamWriter(ostream), true)
        val repl = new HBaseToolsILoop(hbaseOpts, input, output)

        if (sets.classpath.isDefault) {
          sets.classpath.value = sys.props("java.class.path")
        }

        repl process sets
      }
    }
  }

  def run(hbaseOpts: CommandLine, lines: List[String]): String = run(hbaseOpts: CommandLine, lines.map(_ + "\n").mkString)
}








/*
import java.io.BufferedReader
import java.text.SimpleDateFormat
import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.{ IR, ILoop, JPrintWriter }

/**
 * ReplILoop - core of HBaseTools REPL.
 * @param replClassLoader [[ReplClassLoader]] used for runtime/in-memory classloading
 * @param hbaseOpts user arguments for HBase configuration
 */
class ReplILoop(replClassLoader: ReplClassLoader,
                hbaseOpts: CommandLine,
                in: Option[BufferedReader],
                out: JPrintWriter)
    extends ILoop(in, out) {

  def this(replCL: ReplClassLoader, opts: CommandLine) =
    this(replCL, opts, None, new JPrintWriter(Console.out, true))

  // def addThunk(f: => Unit): Unit = intp.initialize(f)

  settings = new GenericRunnerSettings(echo)

  override def prompt: String = /* Console.GREEN + */ "\nhtools> " /* + Console.RESET */

  private def initHbaseShell(): Unit = {
    intp.interpret("val hconf0: org.edma.hbase.HConfiguration = new HConfiguration")
    hbaseOpts.getOptions foreach { opt =>
      val p: String = opt.getOpt
      if (hbaseOpts.hasOption(p)) {
        val v: String = hbaseOpts.getOptionValue(p)
        intp.interpret(f"""hconf0.configure("${p}", "${v}")""")
      }
    }
    intp.interpret("val hsh: org.edma.hbase.HShell = HShell.create(hconf0)")
  }

  /**
   * REPL magic to get a new HBaseTools context using arguments from the command line
   * User may specify a name for the context val, default is `hsh`.
   */
  /*

  // Options for creating new contexts
  private var hbtOpts: Array[String] = args.toArray

  // Hidden magics/helpers for REPL session jars.

  private val createJarCmd = LoopCommand.nullary(
    "createJar", "create a Scio REPL runtime jar",
    () => { Result.resultFromString(replClassLoader.createReplCodeJar) })

  private val getNextJarCmd = LoopCommand.nullary(
    "nextJar", "get the path of the next Scio REPL runtime jar",
    () => { Result.resultFromString(replClassLoader.getNextReplCodeJarPath) })

  private def newScioCmdImpl(name: String) = {
    val sc = if (name.nonEmpty) name else "sc"
    val rsc = "com.spotify.scio.repl.ReplScioContext"
    val opts = optsFromArgs(hbtOpts)
    val nextReplJar = replClassLoader.getNextReplCodeJarPath
    intp.beQuietDuring {
      intp.interpret(s"""val $sc: ScioContext = new $rsc($opts, List("$nextReplJar"))""")
    }
    this.echo("Scio context available as '" + sc + "'")
    Result.default
  }

  */

  // override def commands: List[LoopCommand] = super.commands ++ scioCommands
  override def commands: List[LoopCommand] =
    super.commands ++ hbaseCommands

  // =======================================================================
  // Initialization
  // =======================================================================

  /*
  private def optsFromArgs(args: Seq[String]): String = {
    val factory = "org.apache.beam.sdk.options.PipelineOptionsFactory"
    val options = "org.apache.beam.runners.dataflow.options.DataflowPipelineOptions"
    val argsStr = args.mkString("\"", "\", \"", "\"")
    s"""$factory.fromArgs($argsStr).as(classOf[$options])"""
  }
  */

  private def addImports(): IR.Result =
    intp.interpret(
      """import org.edma.hbase._
        |""".stripMargin)

  /*
  private def loadIoCommands(): IR.Result = {
    intp.interpret(
      """
        |val _ioCommands = new com.spotify.scio.repl.IoCommands(sc.options)
        |import _ioCommands._
      """.stripMargin)
  }
  */

  override def createInterpreter(): Unit = {
    super.createInterpreter()
    intp.beQuietDuring {
      addImports()
      initHbaseShell()
      // newScioCmdImpl("sc")
      // loadIoCommands()
    }
  }

}
*/