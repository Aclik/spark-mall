package com.yxbuild.accu

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CategoryTop10OfSessionIdAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]]{

  private var categoryTop10CountOfSessionId = new mutable.HashMap[String,Long]()

  // 判断是否为空
  override def isZero: Boolean = categoryTop10CountOfSessionId.isEmpty

  // 发送之前进行复制
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accumulator = new CategoryTop10OfSessionIdAccumulator
    accumulator.value ++= categoryTop10CountOfSessionId
    accumulator
  }

  // 重置
  override def reset(): Unit = {
    categoryTop10CountOfSessionId.clear()
  }

  // 区内累加
  override def add(v: String): Unit = {
    categoryTop10CountOfSessionId(v) = categoryTop10CountOfSessionId.getOrElse(v,0L) + 1L
  }

  // 区间合并
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    other.value.foreach(x => {
      val key: String = x._1
      val value: Long = x._2
      categoryTop10CountOfSessionId(key) = categoryTop10CountOfSessionId.getOrElse(key,0L) + value
    })
  }

  // 将结果返回
  override def value: mutable.HashMap[String, Long] = {
    categoryTop10CountOfSessionId
  }
}
