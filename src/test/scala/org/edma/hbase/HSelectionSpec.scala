package org.edma.hbase

import org.scalatest._

class HSelectionSpec extends FreeSpec {

  "A Selection" - {

    "created from a single value (eg \"d:attr\")" - {

      val csel = HSelection("d:attr")

      "produces an HSelection instance" in {
        assert(csel.toString === "'d:attr'")
      }
      "mutates to an HStarSelection when selecting '*'" in {
        val csel1 = csel.select("*")
        assert(csel1.toString === "*")
      }
    }

    "created as a star value (\"*\")" - {
      
      val ssel = HSelection("*")
      "produces an HStarSelection instance" in {
        assert(ssel.toString === "*")
      }
      "voids additional selected columns" in {
        val ssel1 = ssel.select("d:attr")
        assert(ssel1.toString === "*")
      }
    }

  }

  /*
  val esel = HSelection()                         //> esel  : org.edma.hbase.HSelection = 
  
  val lsel = HSelection("d:attr1" :: "d:attr2" :: Nil)
                                                  //> lsel  : org.edma.hbase.HSelection = 'd:attr1', 'd:attr2'
  val lsel1 = lsel.select("d:attr3 ")             //> lsel1  : org.edma.hbase.HSelection = 'd:attr1', 'd:attr2', 'd:attr3 '
  val lsel2 = lsel1.select("d:attr4" :: "d:attr5" :: Nil)
                                                  //> lsel2  : org.edma.hbase.HSelection = 'd:attr1', 'd:attr2', 'd:attr3 ', 'd:at
                                                  //| tr4', 'd:attr5'
   */
}
