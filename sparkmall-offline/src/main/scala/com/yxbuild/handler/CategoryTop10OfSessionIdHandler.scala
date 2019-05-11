package com.yxbuild.handler

import com.yxbuild.accu.CategoryTop10OfSessionIdAccumulator
import com.yxbuild.utils.JdbcUtil
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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
  def getCategoryTop10OfSessionId(spark:SparkSession,categoryTop10List:ListBuffer[mutable.HashMap[String,String]]): ListBuffer[(String,Long)] ={
    // 1、创建累加器对象
    val categoryTop10OfSessionIdAccumulator = new CategoryTop10OfSessionIdAccumulator

    // 2、注册累加器
    spark.sparkContext.register(categoryTop10OfSessionIdAccumulator)

    // 3、获取Top10商品分类的每个SessionId,并且进行累计,数据格式(任务表示_商品分类表示_session标识,点击次数)
    categoryTop10List.foreach(categoryTop10 => {
      // 4、获取任务标识
      val taskId: String = categoryTop10.getOrElse("taskId","") // 任务ID
      // 5、获取商品分类标识
      val categoryId: String = categoryTop10.getOrElse("category_id","") // 商品分类ID
      // 6、根据商品分类标识获取所有点击过该商品分类的Session标识
      val sql = new StringBuilder
      sql.append("select session_id from user_visit_action where 1=1")
      sql.append(s" and click_category_id = '$categoryId'")
      val dataFrame: DataFrame = spark.sql(sql.toString())
      // 7、通过累加器进行累计该商品分类被点击SessionId点击次数
      dataFrame.foreach(x => {
        categoryTop10OfSessionIdAccumulator.add(taskId + "_" + categoryId + "_" + x)
      })
    })

    // 8、获取累加器累加的结果
    val sessIdMap: mutable.HashMap[String, Long] = categoryTop10OfSessionIdAccumulator.value

    // 9、对累加器的结果进行分组,目的为了按照任务标识和商品分类标识进行对用户点击该商品分类的次数进行归类
    val sessionIdGroupMap = sessIdMap.groupBy(x => {
      val key: String = x._1
      val strings: Array[String] = key.split("_")
      strings(0) + "_" + strings(1) // 以任务标识和商品分类标识做为key
    })

    // 10、对分组中的结果进行排序,并且获取每个商品分类被点击次数最多的SessionID进行记录和返回
    val resultList = new ListBuffer[(String,Long)]
    sessionIdGroupMap.foreach(x => {
      // 11、对每个分组的value值进行排序
      val descSessionIdSortMap: List[(String, Long)] = x._2.toList.sortBy(y => {
        y._2
      }).reverse
      // 12、获取点击次数最多的前10个SessionID
      val top10SessionList: List[(String, Long)] = descSessionIdSortMap.take(10)
      // 14、记录每个商品点击的次数和前10个SessionId
      resultList ++= top10SessionList
    })
    resultList
  }

  /**
    * 将数据保存到MySQL数据库
    *
    * @param top10SessionList
    */
  def saveToMySQL(top10SessionList: ListBuffer[(String, Long)]): Unit = {
    // 将前10条数据保存到数据库
    val sql = new StringBuilder
    sql.append("insert into category_session_top10 values(?,?,?,?)")
    val paramList = new ListBuffer[Array[Any]]
    top10SessionList.foreach(x => {
      val idArray: Array[String] = x._1.split("_")
      paramList.append(Array(idArray(0),idArray(1),idArray(2),x._2))
    })
    JdbcUtil.executeBatchUpdate(sql.toString(),paramList)
  }

}
