package com.yxbuild.app

import com.yxbuild.handler.CategoryTop10OfSessionIdHandler
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 需求：Top10品类中,点击次数最多的SessionID和点击次数
  * 1、通过查询MySQL数据库获取Top10商品分类信息
  *
  * 2、通过Hive查询获取点击过Top10的SessionID
  *
  * 3、根据累加器对象点击次数Top10商品分类进行累加，格式:(商品分类ID_SessionId,1)
  *
  * 4、根据商品分类ID进行分组
  *
  * 5、在分组内进行排序
  *
  * 6、获取前10条数据信息
  *
  */
object CategoryTop10OfSessionId {
  def main(args: Array[String]): Unit = {
    // 1、创建sparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("categoryTop10OfSessionId").setMaster("local[*]")

    // 2、创建SparkSession对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("CategoryTop10")
      .enableHiveSupport()
      .getOrCreate()

    // 3、通过查询MySQL数据库获取Top10商品分类信息
    val stringToStrings: ListBuffer[mutable.HashMap[String, String]] = CategoryTop10OfSessionIdHandler.getCategoryTop10ByMySQL(spark)

    // 4、获取Top10商品分类的点击次数最多的相关信息
    val top10SessionList: RDD[List[(String, String, Int)]] = CategoryTop10OfSessionIdHandler.getCategoryTop10OfSessionId(spark,stringToStrings)

    // 5、将信息保存到MySQL数据库
    CategoryTop10OfSessionIdHandler.saveToMySQL(top10SessionList)

    // 6、关闭资源
    spark.close()
  }
}
