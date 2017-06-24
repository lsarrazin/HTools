package org.edma.hbase

/*
 
Usage:

new HQuery(table)
  .select("col1")
  .select("col2")
  .selectCf("l")
  .where key = "rowid"
  .where key in (list Id)
  .where key between (low, high)
  .where key like "pattern"
  .count
  .foreach
  .
  
 */

class HQuery(table: HTable) {
  
  lazy val schema: HSchema = new HSchema(table)
  
  def select(col: String): HQuery = {
    this
  }
  
}