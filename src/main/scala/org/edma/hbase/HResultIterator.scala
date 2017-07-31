package org.edma.hbase

import org.apache.hadoop.hbase.client.{ HTable => HBaseTable, _ }

trait HResultIterator extends Iterator[HResult] {
  
}

class HGetIterator(res: Result)  extends HResultIterator {

  private var hasValue = true
  
  def hasNext: Boolean = hasValue
  
  def next: HResult = {
    if (hasValue) {
      hasValue = false
      new HResult(res)
    } else {
      null
    }
  }
}

class HScanIterator(res: ResultScanner) extends HResultIterator {

  private var hasValue = true
  
  def hasNext: Boolean = false
  
  def next(): HResult = {
    if (hasValue) {
      val sres = res.next()
      if (sres == null) {
        hasValue = false
        null
      } else {
        new HResult(sres)
      }
    } else {
      null
    }
  }
}