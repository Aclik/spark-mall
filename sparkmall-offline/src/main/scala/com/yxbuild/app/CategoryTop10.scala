package com.yxbuild.app

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.yxbuild.dataMode.UserVisitAction
import com.yxbuild.handler.CategoryTop10Handler
import com.yxbuild.utils.PropertiesUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * 需求：根据商品的点击量、订单量、支付量进行排序，并且获取最高的10个商品信息
  *   如果点击量相同，则根据支付量排序，以此类推
  *
  */
object CategoryTop10 {
  def main(args: Array[String]): Unit = {
    // 1、创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CategoryTop10")

    // 2、创建Spark Session
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("CategoryTop10")
      .enableHiveSupport()
      .getOrCreate()

    //  3、获取配置文件的过滤条件信息
    val conditionsStr: Properties = PropertiesUtil.load("conditions.properties")

    // 4、将过滤条件JSON字符串转换成对象
    val conditionsObject: JSONObject = JSON.parseObject(conditionsStr.getProperty("condition.params.json"))

    // 5、读取Hive数据并且进行过滤
    val userVisitActionRDD: RDD[UserVisitAction] = CategoryTop10Handler.readAndFilterData(conditionsObject, sparkSession)
    //userVisitActionRDD.take(10).foreach(println)

    // 6、运用累加器计算用户三种行为(点击、订单、支付)的次数
    val categoryTop10List: List[(String, mutable.HashMap[String, Long])] = CategoryTop10Handler.getCategoryTop10(userVisitActionRDD,sparkSession)
    categoryTop10List.foreach(println)

    // 7、保存商品分类Top10到MySQL数据库
    CategoryTop10Handler.saveCategoryTop10ToMySQL(categoryTop10List)

    // 8、关闭资源
    sparkSession.close()

  }
}
