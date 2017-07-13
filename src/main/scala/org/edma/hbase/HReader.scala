package org.edma.hbase

/**
 * HBase values enumerator
 */
class HReader(conn: HConnection, query: HQuery) {
  
  val table: HTable = query.getFrom.getOrElse(HTable.dummy)
  val columns: HSelection = query.getSelect.getOrElse(HSelection())
  val keys: HKey = query.getWhere.getOrElse(HKey())
  
  lazy val schema: HSchema = HSchema(table)
  
  def explain: Unit = {
    echo("Execution plan of query ")
    echo(query.toString)
  }
}

class HReaderPlan(key: HKey) {
  
  
  
}

