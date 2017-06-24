package org.edma.hbase

import org.scalatest._

class HKeySpec extends FeatureSpec with GivenWhenThen {
  
  info("As a HTools user")
  info("I want to be able create & mutate keys to HBase")
  info("So I can express commands from the REPL")

  feature("HKey creations") {
    scenario("User creates a single key") {
      Given("nothing")
      When("the user declares a key with a single value")
      val key = HKey("123")
      Then("the key is a SingleKey")
      assert(key isSingle)
    }
  }

  feature("HKey mutations") {
    scenario("User appends a range value to a single key") {
      Given("a single key")
      val key = HKey("123")
      assert(key isSingle)
      When("the user calls the to method")
      val nkey = key to "789"
      Then("the resulting key is a range")
      assert(nkey isRange)
    }
  }
}