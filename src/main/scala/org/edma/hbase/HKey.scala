package org.edma.hbase

import collection.immutable.SortedSet

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
  def isInvalid: Boolean = (ktype == Invalid)

  def to(ev: String): HKey = HInvalidKey
  def +(nv: String): HKey = HInvalidKey
  def ++(nvs: Iterable[String]): HKey = HInvalidKey

  def patternToKey(pv: String): String = 
    if (isPattern(pv)) pv.substring(0, pv.length() - 1)
    else pv

  def isPattern(pv: String): Boolean = pv.endsWith("%")
    
  def addKeyToSet(vs: SortedSet[String], nv: String): SortedSet[String] = {
    (vs + nv)
  }

  def addKeysToSet(vs: SortedSet[String], nvs: Iterable[String]): SortedSet[String] = {
    (vs ++ nvs)
  }

}

object HInvalidKey extends HKey {
  def ktype: KeyType = Invalid
  
  override def toString = "Invalid"
}

class HSingleKey(kv: String) extends HKey {
  def ktype: KeyType = Single

  override def to(ev: String): HKey = 
    if (isPattern(ev)) HInvalidKey
    else new HRangeKey(kv, ev)
  
  override def +(nv: String): HKey = new HMultipleKey(SortedSet(kv)) + nv
  override def ++(nvs: Iterable[String]): HKey = new HMultipleKey(SortedSet(kv)) ++ nvs

  override def toString = f"""S"${kv}""""
}

class HPatternKey(kv: String) extends HKey {
  def ktype: KeyType = Pattern
  
  override def to(ev: String): HKey = {
    val sv = patternToKey(kv)
    if (sv < ev) new HRangeKey(patternToKey(kv), ev)
    else HInvalidKey
  }

  override def toString = f"""P"${kv}""""
}

class HMultipleKey(kvs: SortedSet[String]) extends HKey {
  def ktype: KeyType = Multiple
  
  override def to(ev: String): HKey = HInvalidKey
  
  override def +(nv: String): HMultipleKey = new HMultipleKey(addKeyToSet(kvs, nv))
  override def ++(nvs: Iterable[String]): HMultipleKey = new HMultipleKey(addKeysToSet(kvs, nvs.toSet))
  
  override def toString = "M" + kvs.mkString("[\"", "\",\"", "\"]")

}

class HRangeKey(kvs: String, kve: String) extends HKey {
  def ktype: KeyType = Range
  override def toString = f"""R["${kvs}" -> "${kve}"]"""
  
  override def to(ev: String): HKey = {
    if (ev >= kve) new HRangeKey(kvs, ev)
    else HInvalidKey
  }
}

//TODO: Add hybrid key
/* An hybrid key can hold keys from Multiple and Range patterns 
 * For example :
 * val key = HKey("12345", "345%", "67890")
 * */

object HKey {

  def from(kv: String): HKey = HKey(kv)
  def :=(kv: String): HKey = HKey(kv)
  def apply(kv: String): HKey = {
    if (kv contains '%') {
      if (kv endsWith "%") new HPatternKey(kv)
      else HInvalidKey
    }
    else new HSingleKey(kv)
  }

  def :=(kvs: Iterable[String]): HKey = HKey(kvs)
  def apply(kvs: Iterable[String]): HKey = {
    val sset = SortedSet() ++ kvs
    new HMultipleKey(sset)
  }

  def :=(vs: (String, String)): HKey = HKey(vs)
  def apply(vs: (String, String)): HKey = HKey(List(vs._1, vs._2))
  
  def apply(vs: (String, String, String)): HKey = HKey(List(vs._1, vs._2, vs._3))
  def apply(vs: (String, String, String, String)): HKey = HKey(List(vs._1, vs._2, vs._3, vs._4))
  def apply(vs: (String, String, String, String, String)): HKey = HKey(List(vs._1, vs._2, vs._3, vs._4, vs._5))
}
