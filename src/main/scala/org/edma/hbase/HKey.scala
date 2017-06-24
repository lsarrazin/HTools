package org.edma.hbase

object KeyType extends Enumeration {
  type KeyType = Value
  val Single, Multiple, Range, Pattern, Invalid = Value
}
import KeyType._

abstract class HKey {

  def ktype: KeyType

  def isSingle: Boolean = (ktype == Single)
  def isMultiple: Boolean = (ktype == Multiple)
  def isRange: Boolean = (ktype == Range)
  def isPattern: Boolean = (ktype == Pattern)

  def to(ev: String): HKey
  
  def patternToKey(pv: String): String = 
    if (pv.endsWith("%")) pv.substring(0, pv.length() - 1)
    else pv

}

object HInvalidKey extends HKey {
  def ktype: KeyType = Invalid
  
  def to(ev: String): HKey = this
}

class HSingleKey(kv: String) extends HKey {
  def ktype: KeyType = Single

  def to(ev: String): HKey = new HRangeKey(kv, ev)

  override def toString = f"""S"${kv}""""
}

class HPatternKey(kv: String) extends HKey {
  def ktype: KeyType = Pattern
  
  def to(ev: String): HKey = new HRangeKey(patternToKey(kv), ev)

  override def toString = f"""P"${kv}""""
}

class HMultipleKey(kvs: Iterable[String]) extends HKey {
  def ktype: KeyType = Multiple

  //TODO: Handle properly
  def to(ev: String): HKey = HInvalidKey
  
  override def toString = "M" + kvs.mkString("[\"", "\",\"", "\"]")

}

class HRangeKey(kvs: String, kve: String) extends HKey {
  
  def ktype: KeyType = Range
  def to(ev: String): HKey = HInvalidKey
  
  override def toString = f"""R["${kvs}" -> "${kve}"]"""
}

object HKey {

  def apply(kv: String): HKey = {
    if (kv.endsWith("%")) new HPatternKey(kv)
    else new HSingleKey(kv)
  }

  def apply(kvs: Iterable[String]): HKey = {
    new HMultipleKey(kvs)
  }

}
