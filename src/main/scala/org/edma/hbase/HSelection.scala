package org.edma.hbase

abstract class HSelection {
  
  def getColumns: List[String] = Nil
  
  def select(col: String): HSelection
  
  def select(cols: List[String]): HSelection
  
  def checkAgainstSchema(schema: HSchema): Boolean = {
    val cols = getColumns
    
    // TODO: Implement this
    true
  }
}

class HStarSelection extends HSelection {
  
  override def getColumns: List[String] = "*" :: Nil

  override def select(col: String): HSelection = {
    if (col != "*") {
      warn(f"Ineffective column selection: '$col'")
    }
    this
  }
  
  override def select(cols: List[String]): HSelection = {
    warn("Ineffective columns selection:" + cols.mkString("'", "', '", "'"))
    this
  }
  
  override def toString: String = "*"
}

class HEmptySelection extends HSelection {
  
  override def select(col: String): HSelection = 
    if (col == "*") {
      new HStarSelection
    } else {
      new HCustomSelection(col :: Nil)
    }
  
  override def select(cols: List[String]): HSelection = 
    new HCustomSelection(cols)
  
  override def toString: String = ""
}

class HCustomSelection(cols: List[String]) extends HSelection {
  
  override def getColumns: List[String] = cols

  override def select(col: String): HSelection = {
    if (col == "*") {
      warn("Selecting star masks previous selection " + this)
      new HStarSelection
    } else if (cols.contains(col)) {
      this
    } else {
      new HCustomSelection(cols ++ (col :: Nil))
    }
  }
  
  override def select(ncols: List[String]): HSelection = {
    if (ncols.contains("*")) {
      warn("Selecting star masks previous selection " + this)
      new HStarSelection
    } else {
      val acols: List[String] = ncols.filter(c => !cols.contains(c))
      new HCustomSelection(cols ++ acols)
    }
  }
  
  override def toString: String = cols.mkString("'", "', '", "'")
}

class HInvalidSelection extends HSelection {
  
  override def select(col: String): HSelection = this
  
  override def select(ncols: List[String]): HSelection = this
  
  override def toString: String = "<<Invalid>>"
}

object HSelection {
  
  def apply(): HSelection = {
    new HEmptySelection
  }
  
  def apply(col: String): HSelection = {
    if (col == "*") {
      new HStarSelection
    } else {
      new HCustomSelection(col :: Nil)
    }
  }
  
  def apply(cols: List[String]): HSelection = {
    new HCustomSelection(cols)
  }
}