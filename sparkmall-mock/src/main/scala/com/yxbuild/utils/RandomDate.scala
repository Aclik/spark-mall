package com.yxbuild.utils


import java.util.Date

import scala.util.Random

object RandomDate {

  /**
    * 获取随机时间
    *
    * @param startDate 开始时间
    * @param endDate 结束时间
    * @param step
    * @return
    */
  def apply(startDate: Date, endDate: Date, step: Int): RandomDate = {
    val randomDate = new RandomDate()
    val avgStepTime: Long = (endDate.getTime - startDate.getTime) / step //
    randomDate.maxTimeStep = avgStepTime * 2
    randomDate.lastDateTime = startDate.getTime
    randomDate
  }

  /**
    * 定义内部类：产生随机事件
    */
  class RandomDate {
    var lastDateTime = 0L
    var maxTimeStep = 0L

    def getRandomDate: Date = {
      val timeStep: Int = new Random().nextInt(maxTimeStep.toInt)
      lastDateTime = lastDateTime + timeStep
      new Date(lastDateTime)
    }
  }

}
