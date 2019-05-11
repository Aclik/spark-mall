package com.yxbuild.utils

import scala.collection.mutable.ListBuffer
import scala.util.Random

case class RanOpt [T](value: T, weight: Int)
object RandomOptions {
  def apply[T](opts: RanOpt[T]*): RandomOptions[T] = {
    val randomOptions = new RandomOptions[T]()
    for (opt <- opts) {
      randomOptions.totalWeight += opt.weight
      for (i <- 1 to opt.weight) {
        randomOptions.optsBuffer += opt.value
      }
    }
    randomOptions
  }

  def main(args: Array[String]): Unit = {
    val productExRandomOpt: RandomOptions[String] = RandomOptions(RanOpt("自营", 7), RanOpt("第三方", 3))
    println(productExRandomOpt.getRandomOpt)
  }

}

class RandomOptions[T](opts: RanOpt[T]*) {

  var totalWeight = 0
  var optsBuffer = new ListBuffer[T]

  def getRandomOpt: T = {
    val randomNum: Int = new Random().nextInt(totalWeight)
    optsBuffer(randomNum)
  }
}
