package com.yxbuild.handler

import java.util.{Properties, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.yxbuild.dataMode.UserVisitAction
import com.yxbuild.utils.{JdbcUtil, PropertiesUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object SingleJumpRatioHandler {

  /**
    * 获取条件过滤配置文件的JSON对象
    * @param configFileName 配置文件名称
    * @return
    */
  def getConditionJsonObject(configFileName:String ) :JSONObject ={
    // 1、读取配置文件,获取需要计算的页面ID
    val properties: Properties = PropertiesUtil.load("conditions.properties")
    val conditionsStr: String = properties.getProperty("condition.params.json")
    val conditionJsonObject: JSONObject = JSON.parseObject(conditionsStr)
    conditionJsonObject
  }

  /**
    * 获取页面单跳的ID组
    *
    * @param confFile 配置文件名称
    * @return
    */
  def getPageSingleJumpIdGroup(confFile:String):Array[String] = {
    // 1、获取配置文件过滤页面
    val pageIdArray: Array[String] = getConditionJsonObject(confFile).getString("targetPageFlow").split(",")

    // 2、构造单页面的跳转字符
    val fromArray: Array[String] = pageIdArray.dropRight(1)
    val toArray: Array[String] = pageIdArray.drop(1)
    val pageGroupArray: Array[(String, String)] = fromArray.zip(toArray)
    val pageGroupStr: Array[String] = pageGroupArray.map {
      case (from, end) => {
        s"$from-$end"
      }
    }
    pageGroupStr
  }

  /**
    * 获取页面单跳的次数
    *
    * @param spark sparkSession对象
    * @param confFile 配置文件名称
    */
  def getSingleJumpCount(spark:SparkSession,confFile:String):RDD[(String, Long)] = {

    // 1、获取页面单跳的组
    val pageSingleJumpId: Array[String] = getPageSingleJumpIdGroup(confFile)

    // 2、获取数据
    val userVisitActionRDD: RDD[UserVisitAction] = CategoryTop10Handler.readAndFilterData(getConditionJsonObject(confFile), spark)

    // 3、过滤掉非点击数据
    val filterUserVisiActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter {
      case (userVisitAction) => {
        userVisitAction.click_product_id != -1
      }
    }

    // 4、根据sessionId分组
    val groupBySessionIdRDD: RDD[(String, Iterable[UserVisitAction])] = filterUserVisiActionRDD.groupBy(_.session_id)

    // 5、根据点击事件进行升序排序
    val sortByActionTimeRDD: RDD[List[UserVisitAction]] = groupBySessionIdRDD.map {
      case (sessionId, items) => {
        items.toList.sortWith {
          case (item1, item2) => {
            item1.action_time > item2.action_time
          }
        }
      }
    }

    // 6、获取每个SessionId依次点击的页面Id -> List(15, 49, 27, 3, 32, 43, 6, 42, 10)
    val pageIdRDD: RDD[List[Long]] = sortByActionTimeRDD.map {
      case (userVisitActions) => {
        userVisitActions.map(x => {
          x.page_id
        })
      }
    }

    // 7、将SessionId点击页面顺序依次转成元素RDD
    val zipFromAndEndPageIdRDD: RDD[(Long, Long)] = pageIdRDD.flatMap(x => {
      val fromPageIds: List[Long] = x.dropRight(1)
      val endPageIds: List[Long] = x.drop(1)
      fromPageIds.zip(endPageIds)
    })

    // 8、计数
    val jumpPageToOne: RDD[(String, Long)] = zipFromAndEndPageIdRDD.map(x => {
      (s"${x._1}-${x._2}", 1L)
    })


    // 9、过滤不符合的单跳
    val filterSingleJumpRDD: RDD[(String, Long)] = jumpPageToOne.filter(x => {
      pageSingleJumpId.contains(x._1)
    })

    // 10、汇总
    val jumpPageToSum: RDD[(String, Long)] = filterSingleJumpRDD.reduceByKey(_+_)
    jumpPageToSum
  }

  /**
    * 单页面点击的次数
    *
    */
  def getSinglePageCount(spark:SparkSession,confFile:String):  collection.Map[Long, Long] = {
    // 1、获取跳转页面的ID
    val pageIdArray: Array[String] = getConditionJsonObject(confFile).getString("targetPageFlow").split(",")

    // 2、获取数据
    val userVisitActionRDD: RDD[UserVisitAction] = CategoryTop10Handler.readAndFilterData(getConditionJsonObject(confFile), spark)

    // 3、过滤，获取点击目标
    val filterUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(x => {
      pageIdArray.contains(x.page_id.toString)
    })

    // 4、计数
    val filterUserVisitActionToOneRDD: RDD[(Long, Int)] = filterUserVisitActionRDD.map(x =>{
      (x.page_id,1)
    } )

    // 5、汇总
    val countByKey: collection.Map[Long, Long] = filterUserVisitActionToOneRDD.countByKey()
    countByKey
  }

  /**
    * 计算页面单跳转化率
    *
    * @param singleJumpCount 页面单跳的次数
    * @param singlePageCount 页面访问量(PV)
    */
  def getSinglePageJumpOfRate(singleJumpCount: RDD[(String, Long)],singlePageCount:  collection.Map[Long, Long] ): RDD[(String,Double)] = {
    val resultRDD: RDD[(String, Double)] = singleJumpCount.map(x => {
      val key: Long = x._1.split("-")(0).toLong
      val value: Long = singlePageCount.getOrElse(key, 1L)
      val rate: Double = x._2 / value
      (x._1, rate)
    })
    resultRDD
  }

  /**
    * 将数据保存到MySQL数据库
    *
    * @param resultRDD 每个热门点击次数最多的前10为Session
    */
  def saveToMySQL(resultRDD: RDD[(String,Double)] ): Unit = {
    // 将前10条数据保存到数据库
    val sql = new StringBuilder
    sql.append("insert into page_jump_rate values(?,?,?)")
    val paramList = new ListBuffer[Array[Any]]
    resultRDD.collect.foreach(x => {
      paramList.append(Array(UUID.randomUUID().toString,x._1,x._2))
    })

    JdbcUtil.executeBatchUpdate(sql.toString(),paramList)
  }
}
