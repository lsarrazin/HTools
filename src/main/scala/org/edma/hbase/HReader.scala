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

    val table: HTable = query.getFrom.getOrElse(new HDummyTable("ExplainTable"))
    
    val fetcher: Iterable[HFetcher] = HFetcher.forKey(keys, table)
    fetcher.foreach(println)
  }
  
  def execute: Unit = {
    echo("Running query ")
    echo(query.toString)
    
    val table: HTable = query.getFrom.getOrElse(new HInvalidTable("NoTableFromQuery"))
    
    val fetcher: Iterable[HFetcher] = HFetcher.forKey(keys, table)
    fetcher.foreach(fetchRows)    
  }
  
  private def fetchRows(fetcher: HFetcher): Unit = {
    
  }
}

class HReaderPlan(key: HKey) {
  
  
  
}

