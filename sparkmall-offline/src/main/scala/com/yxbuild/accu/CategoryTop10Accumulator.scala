package com.yxbuild.accu

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 累计用户每个行为的次数
  *
  * AccumulatorV2[String,mutable.HashMap[String,Long]]
  * 泛型剖析：
  *   mutable.HashMap[String,Long] 为最终输出格式
  *   String为输入格式
  *
  */
class CategoryTop10Accumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]]{

  /* 定义变量进行接收需要进行累加处理的信息 */
  private var categoryCount = new mutable.HashMap[String,Long]()

  /**
    *
    * 判空 -> 判断是否为空
    *
    * @return
    */
  override def isZero: Boolean = {
    categoryCount.isEmpty
  }

  /**
    * 拷贝 -> 在发送之前先拷贝一份(意义：将当前处理的信息追加到累加器的对象值中,并且进行返回,方便低层进行累加操作)
    * @return
    */
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accumulator = new CategoryTop10Accumulator
    accumulator.value ++= categoryCount // ++= 表示两个Map值的连接,
    accumulator
  }

  /**
    * 重置 -> 拷贝完成之后进行重置
    *
    */
  override def reset(): Unit = {
    categoryCount.clear()
  }

  /**
    * 区内聚合
    *
    * @param v
    */
  override def add(v: String): Unit = {
    categoryCount(v) = categoryCount.getOrElse(v,0L) + 1L // 对用户每次的行为操作进行累加1(通过Key获取当前值,并且在当前值基础上进行加1)
  }

  /**
    * 区间聚合 -- 把other值合并并且追加到变量categoryCount中
    *
    * @param other
    */
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    // 个人写法
    other.value.foreach(x => {
      val key: String = x._1
      val value: Long = x._2
      categoryCount(key) = categoryCount.getOrElse(key,0L) + value
    })

    // 教师写法
   /* val mergePartitionMapValue: mutable.HashMap[String, Long] = this.categoryCount.foldLeft(other.value) {
      case (sessionOther: mutable.HashMap[String, Long], (key, count)) =>
        sessionOther(key) = other.value.getOrElse(key, 0L) + count
        sessionOther
    }*/
    /* 将合并的结果赋值给当前变量,用于最后返回 */
    //this.categoryCount = mergePartitionMapValue
  }

  /**
    * 返回最终结果
    *
    * @return
    */
  override def value: mutable.HashMap[String, Long] = {
    categoryCount
  }
}
