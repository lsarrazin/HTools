package org.edma.hbase

import org.apache.hadoop.hbase.client.{ HTable => HBaseTable, _ }

abstract class HFetcher {
  
  private def getRange(p: String): (String, String) = {
    val i = p.indexOf('%')
    if (i > 0) {
      val c = p.charAt(i-1)
      (p.substring(0, i), p.substring(0, i-1) + (c+1).toChar)
    } else {
      ("", "")
    }
  }

  def forKey(key: HKey, table: HTable): Iterable[HFetcher] = key match {
    case _: HSingleKey => {
      val sv = key.getSingleValue
      if (sv.isDefined) {
        new HGetFetcher(sv.get, table) :: Nil
      } else {
        Nil
      }
    }
    case _: HMultipleKey => {
      val mv = key.getMultipleValues
      if (mv.isDefined) {
        mv.get.map(kv => new HGetFetcher(kv, table))
      } else {
        Nil
      }
    }
    case _: HRangeKey => {
      val rv = key.getRangeValue
      if (rv.isDefined) {
        new HScanFetcher(rv.get, table) :: Nil
      } else {
        Nil
      }
    }
    case _: HPatternKey => {
      val pv = key.getPatternValue
      if (pv.isDefined) {
        new HScanFetcher(getRange(pv.get), table) :: Nil
      } else {
        Nil
      }
    }
    case _ => {
      Nil
    }
  }
  
  def getRowEnumerator(conn: HConnection): Iterator[HResult]
}

class HGetFetcher(rowId: String, table: HTable) extends HFetcher {
  
  def getRowEnumerator(conn: HConnection): Iterator[HResult] = {
    val row: Result = table.get(rowId)
    new HGetIterator(row)
  }
  
  override def toString: String = "Table.get " + rowId
}

class HScanFetcher(rowIds: (String, String), table: HTable) extends HFetcher {
  
  def getRowEnumerator(conn: HConnection): Iterator[HResult] = {
    val row: Result = table.get(rowIds._1)
    val ares = new Array[HResult](1)
    ares(0) = new HResult(row)
    ares.iterator
  }
  
  override def toString: String = "Table.scan " + rowIds._1 + " -> " + rowIds._2
}

class HInvalidFetcher(rowId: String, table: HTable) extends HFetcher {
  
  def getRowEnumerator(conn: HConnection): Iterator[HResult] = {
    new Array[HResult](0).iterator
  }
  
  override def toString: String = "Table.garbage " + rowId
}

object HFetcher {
  
  private def getRange(p: String): (String, String) = {
    val i = p.indexOf('%')
    if (i > 0) {
      val c = p.charAt(i-1)
      (p.substring(0, i), p.substring(0, i-1) + (c+1).toChar)
    } else {
      ("", "")
    }
  }

  def forKey(key: HKey, table: HTable): Iterable[HFetcher] = key match {
    case _: HSingleKey => {
      val sv = key.getSingleValue
      if (sv.isDefined) {
        new HGetFetcher(sv.get, table: HTable) :: Nil
      } else {
        Nil
      }
    }
    case _: HMultipleKey => {
      val mv = key.getMultipleValues
      if (mv.isDefined) {
        mv.get.map(kv => new HGetFetcher(kv, table: HTable))
      } else {
        Nil
      }
    }
    case _: HRangeKey => {
      val rv = key.getRangeValue
      if (rv.isDefined) {
        new HScanFetcher(rv.get, table: HTable) :: Nil
      } else {
        Nil
      }
    }
    case _: HPatternKey => {
      val pv = key.getPatternValue
      if (pv.isDefined) {
        new HScanFetcher(getRange(pv.get), table: HTable) :: Nil
      } else {
        Nil
      }
    }
    case _ => {
      new HInvalidFetcher(key.toString, table: HTable) :: Nil
    }
  }
  
}

