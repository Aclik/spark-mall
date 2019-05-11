package com.yxbuild.handler

import java.util.{Properties, UUID}

import com.alibaba.fastjson.JSON
import com.yxbuild.dataMode.UserVisitAction
import com.yxbuild.utils.{JdbcUtil, PropertiesUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object CategoryTop10OfSessionIdHandler {

  /**
    * 从MySQL数据库获取Top10的点击
    *
    * @param spark
    */
  def getCategoryTop10ByMySQL(spark:SparkSession): ListBuffer[mutable.HashMap[String,String]] = {
    // 1、拼接查询商品分类的前10表的的SQL语句
    val sqlStringBuilder = new StringBuilder
    sqlStringBuilder.append("select * from category_top10 where 1 = 1")

    // 2、获取category_top10表的数据
    val categoryTop10List: ListBuffer[mutable.HashMap[String,String]] = JdbcUtil.query(sqlStringBuilder.toString(),null)
    categoryTop10List
  }

  /**
    * 获取点击次数最多的Session信息
    *
    * @param spark sparkSession实例对象
    * @param categoryTop10List 前10个热门分类
    * @return (任务标识_商品分类标识_SessionId,点击次数)
    */
  def getCategoryTop10OfSessionId(spark:SparkSession,categoryTop10List:ListBuffer[mutable.HashMap[String,String]]): RDD[List[(String, String, Int)]] ={

    // 1、将Top10热门品类转换成RDD -> Map(taskId -> 417db60a-03cc-459e-a2b6-e0fb667cbe83, pay_count -> 329, category_id -> 14, click_count -> 1637, order_count -> 482)
    val categoryTop10RDD: RDD[mutable.HashMap[String, String]] = spark.sparkContext.makeRDD(categoryTop10List)

    // 2、获取过滤数据 -> UserVisitAction(2018-11-26,72,454454f7-9d9c-42ff-aa94-8daeaf87506a,33,2018-11-26 10:51:04,吃鸡,-1,-1,null,null,null,null,21)
    val conditionsStr: Properties = PropertiesUtil.load("conditions.properties")
    val userVisitActionRDD: RDD[UserVisitAction] = CategoryTop10Handler.readAndFilterData(JSON.
      parseObject(conditionsStr.getProperty("condition.params.json")),spark)

    // categoryTop10List.foreach(println) -> Map(taskId -> 417db60a-03cc-459e-a2b6-e0fb667cbe83, pay_count -> 332, category_id -> 1, click_count -> 1696, order_count -> 529)
    // 3、获取Top10的品类ID
    val categoryIdList = new ListBuffer[String]()
    categoryTop10List.foreach(x => {
      categoryIdList.append(x.getOrElse("category_id",""))
    })

    // 3、过滤数据---获取点击到Top10品类的用户行为信息
    val filterUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(x => {
      categoryIdList.contains(x.click_category_id.toString)
    })

    // 4、计数 -> (categoryId_session_Id,1)
    val countToOneRDD: RDD[(String, Int)] = filterUserVisitActionRDD.map(x => {
      (s"${x.click_category_id}_${x.session_id}", 1)
    })

    // 5、算和
    val countToSumRDD: RDD[(String, Int)] = countToOneRDD.reduceByKey(_+_)

    // 6、重新配置维度(品类ID，sessionId,点击总数)
    val resetRDD: RDD[(String, String, Int)] = countToSumRDD.map(x => {
      (x._1.split("_")(0), x._1.split("_")(1), x._2)
    })

    // 6、根据品类进行分组
    val groupRDD: RDD[(String, Iterable[(String, String, Int)])] = resetRDD.groupBy(x => {
      x._1
    })

    // 7、排序
    val sortRDD: RDD[List[(String, String, Int)]] = groupRDD.map(x => {
      x._2.toList.sortWith {
        case (item1, item2) => {
          item1._3 > item2._3
        }
      }
    })

    // 8、获取前10
    val top10SessionId: RDD[List[(String, String, Int)]] = sortRDD.map(x => {
      x.take(10)
    })
    top10SessionId
  }

  /**
    * 将数据保存到MySQL数据库
    *
    * @param top10SessionRDD 每个热门点击次数最多的前10为Session
    */
  def saveToMySQL(top10SessionRDD: RDD[List[(String, String, Int)]] ): Unit = {
    // 将前10条数据保存到数据库
    val sql = new StringBuilder
    sql.append("insert into category_session_top10 values(?,?,?,?)")
    val paramList = new ListBuffer[Array[Any]]
    top10SessionRDD.collect().foreach(x => {
      x.foreach(y => {
        paramList.append(Array(UUID.randomUUID().toString,y._1,y._2,y._3))
      })
    })
    JdbcUtil.executeBatchUpdate(sql.toString(),paramList)
  }

}
