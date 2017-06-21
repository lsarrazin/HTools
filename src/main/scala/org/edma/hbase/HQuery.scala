package org.edma.hbase

class HQuery(table: HTable) {
  
  lazy val schema: HSchema = new HSchema(table)
  
  
  
}