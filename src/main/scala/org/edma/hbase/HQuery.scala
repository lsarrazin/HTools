package org.edma.hbase

/*
 
Usage:

new HQuery
  .select("col1")
  .select("col2")
  .selectCf("l")
  .from("table")
  .where key = "rowid"
  .where key in (list Id)
  .where key between (low, high)
  .where key like "pattern"
  .count
  .foreach
  .
  
 */

/** Generic HBase Query builder */
trait HQuery {
  
  /** Test if query is runnable (only complete queries are) */
  def isRunnable: Boolean = false
  
  /** Retrieve selection */
  def getSelect: Option[HSelection] = None
  /** Retrieve source table */
  def getFrom: Option[HTable] = None
  /** Retrieve key */
  def getWhere: Option[HKey] = None
  
  /** Append selection */
  def select(sel: HSelection): HQuery = new HSQuery(sel)
  /** Append source table */
  def from(tab: HTable): HQuery = new HFQuery(tab)
  /** Append key */
  def where(key: HKey): HQuery = new HWQuery(key)
  
  /** Natural selection appender from column name */
  def select(col: String): HQuery = {
    val selection = HSelection(col)
    select(selection)
  }
  /** Natural selection appender from columns list */
  def select(cols: Product): HQuery = {
    val selection = HSelection(cols)
    select(selection)
  }
  
  /** Natural key appender from string */
  def where(key: String): HQuery = {
    val hkey = HKey(key)
    where(hkey)
  }
  /** Natural key appender from list of values */
  def where(keys: Iterable[String]): HQuery = {
    val hkey = HKey(keys)
    where(hkey)
  }
  
  /** Natural table appender from string */
  def from(table: String)(implicit conn: HConnection): HQuery = {
    val htable = HTable(conn, table)
    from(htable)
  }
  
  /** Query as string for human reading */
  override def toString: String = {
    def queryPart(prefix: String, option: Option[AnyRef], subst: String) = {
      if (option.isDefined) {
        prefix + " " + option.get
      } else {
        prefix + " " + subst
      }
    }
    
    queryPart("SELECT", getSelect, "???") + "\n" +
    queryPart("FROM", getFrom, "???") + "\n" + 
    queryPart("WHERE", getWhere, "???")
  }

  /** Retrieves built status error message */
  def queryError: String = "Query is not complete"
  
  def toIterable[X](implicit conn: HConnection): Iterable[X] = {
    if (!isRunnable) {
      error(queryError)
      Nil
    } else {
      Nil
    }
  }
}

class HVoidQuery extends HQuery {

  /** Retrieves built status error message */
  override def queryError: String = "Query is void"

}

class HSQuery(select: HSelection) extends HQuery {
  
  override def getSelect: Option[HSelection] = Option(select)

  override def select(sel: HSelection): HQuery = new HSQuery(select ++ sel)
  override def from(tab: HTable): HQuery = new HSFQuery(select, tab)
  override def where(key: HKey): HQuery = new HSWQuery(select, key)

  /** Retrieves built status error message */
  override def queryError: String = "Query lacks FROM and WHERE clauses"
}

class HFQuery(table: HTable) extends HQuery {
  
  override def getFrom: Option[HTable] = Option(table)
  
  override def select(sel: HSelection): HQuery = new HSFQuery(sel, table)
  override def from(tab: HTable): HQuery = {
    warn("JOIN queries are not supported (yet)")
    this
  }
  override def where(key: HKey): HQuery = new HFWQuery(table, key)

  /** Retrieves built status error message */
  override def queryError: String = "Query lacks SELECT and WHERE clauses"
}

class HWQuery(key: HKey) extends HQuery {
  
  override def getWhere: Option[HKey] = Option(key)

  override def select(sel: HSelection): HQuery = new HSWQuery(sel, key)
  override def from(tab: HTable): HQuery = new HFWQuery(tab, key)
  override def where(nkey: HKey): HQuery = new HWQuery(key or nkey)

  /** Retrieves built status error message */
  override def queryError: String = "Query lacks SELECT and FROM clauses"
}

class HSFQuery(select: HSelection, table: HTable) extends HQuery {

  override def getSelect: Option[HSelection] = Option(select)
  override def getFrom: Option[HTable] = Option(table)

  override def select(sel: HSelection): HQuery = new HSFQuery(select ++ sel, table)
  override def from(tab: HTable): HQuery = {
    warn("JOIN queries are not supported (yet)")
    this
  }
  override def where(key: HKey): HQuery = new HSFWQuery(select, table, key)

  /** Retrieves built status error message */
  override def queryError: String = "Query lacks WHERE clause"
}

class HSWQuery(select: HSelection, key: HKey) extends HQuery {

  override def getSelect: Option[HSelection] = Option(select)
  override def getWhere: Option[HKey] = Option(key)

  override def select(sel: HSelection): HQuery = new HSWQuery(select ++ sel, key)
  override def from(table: HTable): HQuery = new HSFWQuery(select, table, key)
  override def where(nkey: HKey): HQuery = new HSWQuery(select, key or nkey)

  /** Retrieves built status error message */
  override def queryError: String = "Query lacks FROM clause"
}

class HFWQuery(table: HTable, key: HKey) extends HQuery {
  
  override def getFrom: Option[HTable] = Option(table)
  override def getWhere: Option[HKey] = Option(key)

  /** Retrieves built status error message */
  override def queryError: String = "Query lacks SELECT clause"
}

class HSFWQuery(select: HSelection, table: HTable, key: HKey) extends HQuery {
  
  /** Only complete query is runnable */
  override def isRunnable: Boolean = true
  
  override def select(sel: HSelection): HQuery = new HSFWQuery(select ++ sel, table, key)
  override def from(tab: HTable): HQuery = {
    warn("JOIN queries are not supported (yet)")
    this
  }
  override def where(nkey: HKey): HQuery = new HSFWQuery(select, table, key or nkey)

  override def getSelect: Option[HSelection] = Option(select)
  override def getFrom: Option[HTable] = Option(table)
  override def getWhere: Option[HKey] = Option(key)
  
  /** Retrieves built status error message */
  override def queryError: String = "Query seems complete"

}

object HQuery {
  
  // Default constructor
  def apply(): HQuery = new HVoidQuery()
  
  // Create using selection
  def select(col: String): HQuery = {
    val selection = HSelection(col)
    new HSQuery(selection)
  }
  
  // Create using table
  def from(table: HTable): HQuery = {
    new HFQuery(table)
  }
  def from(table: String)(implicit conn: HConnection): HQuery = {
    new HFQuery(HTable(conn, table))
  }
}