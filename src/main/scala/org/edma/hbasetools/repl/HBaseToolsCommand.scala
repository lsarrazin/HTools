package org.edma.hbasetools.repl

case class HBaseToolsCommand(name: String, code: String, syntax: String, shelp: String, lhelp: String) {
  
  def getName: String = name
  def getCode: String = code
  
  def getLine: String = f"""$name\t$shelp"""
  def getHelp: String = f"""$name\n$syntax\n$lhelp"""
}

trait HBaseToolsCommandProvider {
  def getShellCommands: List[HBaseToolsCommand]
}