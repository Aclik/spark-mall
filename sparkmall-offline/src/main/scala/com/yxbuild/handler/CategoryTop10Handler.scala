package com.yxbuild.handler

import java.util.UUID

import com.alibaba.fastjson.JSONObject
import com.yxbuild.accu.CategoryTop10Accumulator
import com.yxbuild.dataMode.UserVisitAction
import com.yxbuild.utils.JdbcUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object CategoryTop10Handler {



  /**
    * 读取和过滤数据
    *
    * @param conditionsJson 过滤条件
    * @param spark SparkSession对象
    */
  def readAndFilterData(conditionsJson:JSONObject,spark:SparkSession): RDD[UserVisitAction] = {

    // 1、导入隐式转换,此处的spark为SparkSession对象,并非包名
    import spark.implicits._

    // 2、获取过滤条件
    val startDate: String = conditionsJson.getString("startDate")
    val endDate: String = conditionsJson.getString("endDate")
    val startAge: String = conditionsJson.getString("startAge")
    val endAge: String = conditionsJson.getString("endAge")

    // 3、构建SQL语句
    val sqlBuilder = new StringBuilder("select t.* from user_visit_action t join user_info u on u.userId = t.user_id where 1=1")
    if (!StringUtils.isEmpty(startDate)) {
      sqlBuilder.append(s" and date >= '$startDate'")
    }
    if (!StringUtils.isEmpty(endDate)) {
      sqlBuilder.append(s" and date <= '$endDate'")
    }
    if (!StringUtils.isEmpty(startAge)) {
      sqlBuilder.append(s" and age >= '$startAge'")
    }
    if (!StringUtils.isEmpty(endAge)) {
      sqlBuilder.append(s" and age <= '$endAge'")
    }
    //println(sqlBuilder.toString())

    // 4、执行SQL查询
    val dataFrame: DataFrame = spark.sql(sqlBuilder.toString())
    //dataFrame.show(10)
    dataFrame.as[UserVisitAction].rdd
  }

  /**
    * 获取商品分类Top10的数据
    *
    * @param userVisitActionRDD 源数据RDD
    */
  def getCategoryTop10(userVisitActionRDD:RDD[UserVisitAction],spark:SparkSession): List[(String, mutable.HashMap[String, Long])] = {
    // 1、创建累加器对象
    val categoryTop10Accumulator = new CategoryTop10Accumulator

    // 2、注册累加器
    spark.sparkContext.register(categoryTop10Accumulator)

    // 3、运用累加器进行运算(点击、订单、支付行为)
    userVisitActionRDD.foreach(userVisitAction => {
      if (userVisitAction.click_category_id != -1) { // 如果点击商品分类的ID不为-1,则该商品分类点击行为次数加1
        categoryTop10Accumulator.add(s"click_${userVisitAction.click_category_id}")
      }else if (userVisitAction.order_category_ids != null) { // 如果不是点击行为，则判断是否为订单行为，如果是订单行为不为null，则该产品分类订单行为数加1
        // 由于订单可能为多个,所以需要根据格式进行切割
        userVisitAction.order_category_ids.split(",").foreach(categoryId => {
          categoryTop10Accumulator.add(s"order_$categoryId")
        })
      }else if (userVisitAction.pay_category_ids != null) {// 如果不是点击和订单行为，则判断是否为支付行为，如果是支付行为不为null，则该产品分类支付行为数加1
        userVisitAction.pay_category_ids.split(",").foreach(categoryId => {
          categoryTop10Accumulator.add(s"pay_$categoryId")
        })
      }
    })

    // 4、获取累加器中的值,数据格式案例：(pay_17,340)
    val categoryCountMap: mutable.HashMap[String, Long] = categoryTop10Accumulator.value

    // 5、按照商品类ID进行分组,数据格式案例：(8,Map(click_8 -> 1627, pay_8 -> 362, order_8 -> 499))
    val categoryGroupMap: Map[String, mutable.HashMap[String, Long]] = categoryCountMap.groupBy(x => x._1.split("_")(1))

    // 6、按照点击、订单、支付所累积的次数进行升序排序(需要将Map转换为List才能调用sortWith函数)
    val sortCategoryList: List[(String, mutable.HashMap[String, Long])] = categoryGroupMap.toList.sortWith((v1, v2) => {
      val categoryCountMap1 = v1._2
      val categoryCountMap2 = v2._2
      // 根据点击行为次数进行排序
      if (categoryCountMap1.getOrElse(s"click_${v1._1}", 0L) > categoryCountMap2.getOrElse(s"click_${v2._1}", 0L)) {
        true
        // 点击行为次相同，再根据订单行为次数进行排序
      }else if (categoryCountMap1.getOrElse(s"click_${v1._1}", 0L) == categoryCountMap2.getOrElse(s"click_${v2._1}", 0L)){
        if (categoryCountMap1.getOrElse(s"order_${v1._1}", 0L) > categoryCountMap2.getOrElse(s"order_${v2._1}", 0L)) {
          true
          // 订单和点击行为次数相同，再根据支付行为次数进行排序
        }else if (categoryCountMap1.getOrElse(s"order_${v1._1}", 0L) == categoryCountMap2.getOrElse(s"order_${v2._1}", 0L)) {
          if(categoryCountMap1.getOrElse(s"pay_${v1._1}", 0L) > categoryCountMap2.getOrElse(s"order_${v2._1}", 0L)) {
            true
          }else {
            false
          }
        }else {
          false
        }
      }else {
        false
      }
    })

    // 7、获取前10条，并且转将数据转换成数组
    val top10CategoryList: List[(String, mutable.HashMap[String, Long])] = sortCategoryList.take(10)
    top10CategoryList
  }

  /**
    * 将Top10的信息保存到MySQL
    *
    * @param top10CategoryList 根据用户行为，Top10的数据
    */
  def saveCategoryTop10ToMySQL(top10CategoryList: List[(String, mutable.HashMap[String, Long])]): Unit ={
    // 1、将数据遍历成数组List集合
    val taskId: String = UUID.randomUUID().toString
    val paramsList: List[Array[Any]] = top10CategoryList.map(x => {
      Array(taskId, x._1, x._2.getOrElse(s"click_${x._1}",0L), x._2.getOrElse(s"order_${x._1}",0L), x._2.getOrElse(s"pay_${x._1}",0L))
    })
    // 2、将前10条保存到MySQL数据库中
    val sqlStringBuilder = new StringBuilder
    sqlStringBuilder.append("insert into category_top10 value(?,?,?,?,?)")
    JdbcUtil.executeBatchUpdate(sqlStringBuilder.toString(),paramsList)
  }

}
