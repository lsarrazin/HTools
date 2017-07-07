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

/**
 * Implements columns selection
 */
trait HQueryColumnSelection {
  
}

object HSampleQuery {
  
}

/**
 * Implements rows selection
 */
trait HQueryRowSelection {
  
}

class HQuery(table: HTable, selection: HSelection, key: HKey)