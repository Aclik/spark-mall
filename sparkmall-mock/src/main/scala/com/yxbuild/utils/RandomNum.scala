package com.yxbuild.utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object RandomNum {

  def apply(fromNum: Int, toNum: Int): Int = {
    fromNum + new Random().nextInt(toNum - fromNum + 1)
  }

  /**
    *  获取随机数字串
    *
    * @param fromNum 开始数字
    * @param toNum 结束数字
    * @param amount 产生随机数的个数
    * @param delimiter 分隔符
    * @param canRepeat 是否重复
    * @return
    */
  def multi(fromNum: Int, toNum: Int, amount: Int, delimiter: String, canRepeat: Boolean): String = {
    var str = ""
    if (canRepeat) {
      val buffer = new ListBuffer[Int]
      while (buffer.size < amount) {
        val randoNum: Int = fromNum + new Random().nextInt(toNum - fromNum)
        buffer += randoNum
        str = buffer.mkString(delimiter)
      }

    } else {
      val set = new mutable.HashSet[Int]()
      while (set.size < amount) {
        val randoNum: Int = fromNum + new Random().nextInt(toNum - fromNum)
        set += randoNum
        str = set.mkString(delimiter)
      }
    }
    str
  }

}
