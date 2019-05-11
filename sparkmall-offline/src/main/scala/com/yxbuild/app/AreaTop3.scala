package com.yxbuild.app

import com.yxbuild.handler.AreaTop3Handler
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 需求：各个地区点击量最多的3个商品信息，以及每个城市所占的比例
  *
  * 1、根据用户行为表进行关联商品表、城市表，获取商品名称、商品ID、地区ID、地区名称、城市名称
  *
  * 2、根据商品ID和地区ID进行分组，从而获取每个该商品在该地区所被点击的总次数
  *
  * 3、根据地区ID、商品ID、城市ID进行分组，获取每个城市该商品被点击的次数；
  *
  */
object AreaTop3 {
  def main(args: Array[String]): Unit = {
    // 1、创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CategoryTop10")

    // 2、创建Spark Session
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("CategoryTop10")
      .enableHiveSupport()
      .getOrCreate()

    // 3、获取数据
    AreaTop3Handler.getAreaAllDataBackRDD(sparkSession)
    sparkSession.close()
  }
}
