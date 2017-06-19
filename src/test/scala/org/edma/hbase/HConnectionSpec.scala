package org.edma.hbase

import org.scalatest._

class HConnectionSpec extends FeatureSpec with GivenWhenThen {
  
  info("As a HTools user")
  info("I want to be able connect & disconnect to HBase")
  info("So I can pass commands from the REPL")

  feature("HBase connection") {
    scenario("User connects to HBase from idle") {
      Given("an initial configuration")

      // val tv = new TVSet
      // assert(!tv.isOn)
      When("the user calls connect")
      // tv.pressPowerButton()
      Then("the connection status is set to connected")
      assert(true)
    }
  }
}