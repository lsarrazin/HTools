package org.edma.hbase

import org.scalatest._

class HKeySpec extends FreeSpec {

  "A Key" - {

    "created from a single value (eg \"Hello\")" - {

      val skey = HKey("Hello")

      "produces an HSingleKey instance" in {
        assert(skey.isSingle)
        assert(skey.toString === "S\"Hello\"")
      }
      "mutates to a range key using to operator" in {
        val skey2 = skey to "World"
        assert(skey2.isRange)
        assert(skey2.toString === "R[\"Hello\" -> \"World\"]")
      }
      "mutates to a multiple key using + operator" in {
        val skey3 = skey + "World"
        assert(skey3.isMultiple)
        assert(skey3.toString === "M[\"Hello\",\"World\"]")
      }
      "mutates to a multiple key using ++ operator" in {
        val skey4 = skey ++ List("Bonjour", "Monde")
        assert(skey4.isMultiple)
        assert(skey4.toString === "M[\"Bonjour\",\"Hello\",\"Monde\"]")
      }
    }

    "created as a multiple value" - {

      val skey = HKey("Hello")

    }

    "created from a pattern value like \"Hello%\"" - {

      val pkey = HKey("Hello%")

      "produces an HPatternKey instance" in {
        assert(pkey.isPattern)
        assert(pkey.toString === "P\"Hello%\"")
      }
      "unless '%' found before last char" in {
        val wkey1 = HKey("%Hello")
        assert(wkey1.isInvalid)
        val wkey2 = HKey("Hel%lo")
        assert(wkey1.isInvalid)
      }
      "mutates to a range key using to operator" - {

        "if new bound is after pattern" in {
            val pkey2 = pkey to "World"
            assert(pkey2.isRange)
            assert(pkey2.toString === "R[\"Hello\" -> \"World\"]")
        }
        
        "but fails if new bound is before pattern" in {
          val pkey3 = pkey to "A"
          assert(pkey3.isInvalid)
          assert(pkey3 == HInvalidKey)
          assert(pkey3.toString === "Invalid")
        }
      }
    }

    "created as a range key" - {
      val rkey = HKey from ("ABC") to ("DEF")
      val rkey2 = rkey to "XYZ" //> rkey3  : org.edma.hbase.HKey = Invalid
      val rkey4 = rkey to "BAA" //> rkey4  : org.edma.hbase.HKey = Invalid
      
      "produces a range key" in {
        assert(rkey.isRange)
        assert(rkey.toString === "R[\"ABC\" -> \"DEF\"]")
      }
      "extends to a new range" - {
        
        "when extension is after upper bound" in {
          val rkey2 = rkey to "XYZ"
          assert(rkey2.isRange)
          assert(rkey2.toString === """R["ABC" -> "XYZ"]""")
        }
        "but fails otherwise" in {
          val rkey3 = rkey to "BCD"
          assert(rkey3.isInvalid)
          assert(rkey3 == HInvalidKey)
          assert(rkey3.toString === "Invalid")
        }
      }
    }

    val mkey = HKey(List("ABC", "DEF")) //> mkey  : org.edma.hbase.HKey = M["ABC","DEF"]
    val mkeyd = HKey(List("789", "456", "123", "456", "789")) //> mkeyd  : org.edma.hbase.HKey = M["123","456","789"]
    val mkeyo = HKey(List("789", "456", "123")) //> mkeyo  : org.edma.hbase.HKey = M["123","456","789"]
    val mkeyo2 = mkeyo + "456" //> mkeyo2  : org.edma.hbase.HKey = M["123","456","789"]

    val mkey2 = mkey to "XYZ" //> mkey2  : org.edma.hbase.HKey = Invalid
    val mkey3 = mkey + "XYZ" //> mkey3  : org.edma.hbase.HKey = M["ABC","DEF","XYZ"]
    val mkey4 = mkey3 ++ List("GHI", "JKL") //> mkey4  : org.edma.hbase.HKey = M["ABC","DEF","GHI","JKL","XYZ"]

    val tkey = HKey("123", "456") //> tkey  : org.edma.hbase.HKey = M["123","456"]
    val tkeyd = HKey("789", "456", "123", "456", "789")
    //> tkeyd  : org.edma.hbase.HKey = M["123","456","789"]  // Describe a scope for a subject, in this case: "A Set"

  }
}
