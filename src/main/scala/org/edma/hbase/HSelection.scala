package org.edma.hbase

abstract class HSelection {

  def getColumns: Iterable[String] = Nil

  def select(col: String): HSelection

  def select(cols: Iterable[String]): HSelection
  
  def select(cols: Product): HSelection = {
    this ++ HSelection(cols)
  }

  def ++(nsel: HSelection): HSelection

  def checkAgainstSchema(schema: HSchema): Boolean = {
    val cols = getColumns

    // TODO: Implement this
    true
  }
}

class HStarSelection extends HSelection {

  override def getColumns: Iterable[String] = "*" :: Nil

  override def select(col: String): HSelection = {
    if (col != "*") {
      warn(f"Ineffective column selection: '$col'")
    }
    this
  }

  override def select(cols: Iterable[String]): HSelection = {
    warn("Ineffective columns selection:" + cols.mkString("'", "', '", "'"))
    this
  }

  override def ++(nsel: HSelection): HSelection = {
    warn("Ineffective colums selection:" + nsel)
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

  override def select(cols: Iterable[String]): HSelection =
    new HCustomSelection(cols)

  override def ++(nsel: HSelection): HSelection = nsel

  override def toString: String = ""
}

class HCustomSelection(cols: Iterable[String]) extends HSelection {

  override def getColumns: Iterable[String] = cols

  override def select(col: String): HSelection = {
    if (col == "*") {
      warn("Selecting star masks previous selection " + this)
      new HStarSelection
    } else if (cols.exists(_ == col)) {
      this
    } else {
      new HCustomSelection(cols ++ (col :: Nil))
    }
  }

  override def select(ncols: Iterable[String]): HSelection = {
    if (ncols.exists(_ == "*")) {
      warn("Selecting star masks previous selection " + this)
      new HStarSelection
    } else {
      val acols: Iterable[String] = ncols.filter(c => !cols.exists(_ == c))
      new HCustomSelection(cols ++ acols)
    }
  }

  override def ++(nsel: HSelection): HSelection = nsel match {
    case _: HEmptySelection => this
    case _: HStarSelection  => nsel
    case _: HCustomSelection => {
      select(nsel.getColumns)
    }
  }

  override def toString: String = cols.mkString("'", "', '", "'")
}

class HInvalidSelection extends HSelection {

  override def select(col: String): HSelection = this

  override def select(ncols: Iterable[String]): HSelection = this

  override def ++(nsel: HSelection): HSelection = this

  override def toString: String = "<<Invalid>>"
}

object HSelection {

  def apply(): HSelection = {
    new HEmptySelection
  }

  /*
  def apply(col: String): HSelection = {
    if (col == "*") {
      new HStarSelection
    } else {
      new HCustomSelection(col :: Nil)
    }
  }
  
  def apply(cols: Iterable[String]): HSelection = {
    new HCustomSelection(cols)
  }
  
  def apply(cols: Product): HSelection = {
    val columns: Iterable[String] = cols.productIterator.toIterable.map(_.toString)
    new HCustomSelection(columns)   
  }*/

  def apply(any: Any): HSelection = any match {
    case c: String => {
      if (c == "*") {
        new HStarSelection
      } else {
        new HCustomSelection(c :: Nil)
      }
    }

    case cp: Product => {
      val cols: Iterable[String] = cp.productIterator.toIterable.map(_.toString)
      new HCustomSelection(cols)
    }
    
    case cs: Iterable[Any] => new HCustomSelection(cs.map(_.toString))
    
    case _ => {
      error("Unable to figure out what " + any + " means")
      new HInvalidSelection
    }
  }

}