package com.yxbuild.app

import com.yxbuild.dataMode.UserVisitAction
import com.yxbuild.handler.SingleJumpRatioHandler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 页面单跳转换率
  * 计算公式：页面单跳转化率/页面的访问量，比如:2-3跳转数/2页面访问量 = 转化率
  *
  */
object SingleJumpRatioApp {
  def main(args: Array[String]): Unit = {
    // 1、获取SparkSession实例
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("singleJumpRatioApp")
      .enableHiveSupport().getOrCreate()

    // 2、页面单跳的次数
    val singleJumpCount: RDD[(String, Long)] = SingleJumpRatioHandler.getSingleJumpCount(spark,"conditions.properties")

    // 3、获取页面单点的次数
    val singlePageCount:  collection.Map[Long, Long] = SingleJumpRatioHandler.getSinglePageCount(spark,"conditions.properties")

    // 4、计算页面单跳转化率
    val resultRDD: RDD[(String, Double)] = SingleJumpRatioHandler.getSinglePageJumpOfRate(singleJumpCount,singlePageCount)

    // 5、将数据保存到数据库
    SingleJumpRatioHandler.saveToMySQL(resultRDD)

    // 6、关闭资源
    spark.close()
  }
}
