package org.ctl.kafka

import scala.util.Random
import net.andreinc.mockneat.types.enums.CharsType._
import net.andreinc.mockneat.unit.types.Chars.chars
import scala.collection.JavaConverters._

object Model {
  private val syms = List("AAPL", "MSFT", "IBM", "SPY")
  private val rand = new Random

  def randomSymbol = syms(rand.nextInt(syms.length))
}

case class Stock(symbol: String, defaultAllocationType: String)
object Stock {
  def random() = {
    Stock(Model.randomSymbol, chars().`type`(UPPER_LETTERS).collection(3).get().asScala.mkString)
  }
}

case class OtherStock(symbol: String, pmm: String)
object OtherStock {
  def random() = {
    OtherStock(Model.randomSymbol, chars().`type`(UPPER_LETTERS).get() + "")
  }
}



