package com.yxbuild.handler

import com.yxbuild.dataMode.ProductAndArea
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object AreaTop3Handler {

  /**
    * 获取所有数据
    *
    * @param spark
    * @return
    */
  def getAreaAllDataBackRDD(spark:SparkSession): RDD[ProductAndArea] ={
    // 0、导入隐式转换
    import spark.implicits._

    // 1、定义SQL语句
    val sqlBuilder = new StringBuilder
    sqlBuilder.append("select p.product_id,p.product_name,c.* from user_visit_action t join city_info c")
    sqlBuilder.append(" on t.city_id = c.city_id join product_info p on p.product_id = t.click_product_id")

    // 2、查询数据
    val dataFrame: DataFrame = spark.sql(sqlBuilder.toString())

    // 3、将DataFrame转为RDD,并且返回
    val productAndAreaRDD: RDD[ProductAndArea] = dataFrame.as[ProductAndArea].rdd // 需要导入隐式转换
    /* =========================================================== */
    // 4、按地区名称和商品名称进行分组
    val productNameAndAreaRDD: RDD[(String, Iterable[ProductAndArea])] = productAndAreaRDD.groupBy(x => {
      x.area + "|" + x.product_name
    })

    // 5、获取每个商品在每个地区被点击的次数
    val countRDD: RDD[(String, Int)] = productNameAndAreaRDD.map(x => {
      (x._1, x._2.size)
    })

    // 6、根据地区进行分组
    val areaGroupRDD: RDD[(String, Iterable[(String, Int)])] = countRDD.groupBy(x => {
      x._1.split("|")(1)
    })

    // 7、排序
    areaGroupRDD.map(x => {
      val sortByClickNumList: List[(String, Int)] = x._2.toList.sortBy(y => {
        y._2
      }).reverse
      sortByClickNumList.take(3).foreach(println)
    })

    // 6、根据地区名称、商品名称、城市名称进行分组
    val countProductRDD: RDD[(String, Iterable[ProductAndArea])] = productAndAreaRDD.groupBy(x => {
      x.area + "|" + x.product_name + "|" + x.city_name
    })


    // 7、


    productAndAreaRDD
  }


}
