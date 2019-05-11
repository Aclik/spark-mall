package com.yxbuild.utils

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import javax.sql.DataSource
import com.alibaba.druid.pool.DruidDataSourceFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object JdbcUtil {

  /* 初始化连接数据源 */
  var dataSource: DataSource = init()

  def init(): DataSource = {
    val properties = new Properties()
    val config: Properties = PropertiesUtil.load("config.properties")
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", config.getProperty("jdbc.url"))
    properties.setProperty("username", config.getProperty("jdbc.user"))
    properties.setProperty("password", config.getProperty("jdbc.password"))
    properties.setProperty("maxActive", config.getProperty("jdbc.datasource.size"))
    DruidDataSourceFactory.createDataSource(properties)
  }

  /**
    * 单条数据更新
    *
    * @param sql 更新SQL语句
    * @param params 参数值
    * @return
    */
  def executeUpdate(sql: String, params: Array[Any]): Int = {
    var rtn = 0
    var pstmt: PreparedStatement = null
    val connection: Connection = dataSource.getConnection
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)

      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  /**
    * 批量更新数据
    *
    * @param sql SQL语句
    * @param paramsList 值
    * @return
    */
  def executeBatchUpdate(sql: String, paramsList: Iterable[Array[Any]]): Array[Int] = {
    var rtn: Array[Int] = null
    var pstmt: PreparedStatement = null
    val connection: Connection = dataSource.getConnection
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      for (params <- paramsList) {
        if (params != null && params.length > 0) {
          for (i <- params.indices) {
            pstmt.setObject(i + 1, params(i))
          }
          pstmt.addBatch()
        }
      }
      rtn = pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  /**
    * 通过SQL语句查询数据库信息
    *
    * @param sql
    */
  def query(sql:String,params: Array[Any]): ListBuffer[mutable.HashMap[String,String]] = {
    // 1、获取MySQL的连接
    val connection: Connection = dataSource.getConnection

    // 2、配置PreparedStatement
    val statement: PreparedStatement = connection.prepareStatement(sql)
    if (params != null && params.length > 0) {
      for (i <- params.indices) {
        statement.setObject(i + 1, params(i))
      }
    }

    // 3、执行SQL语句并且接受查询结果
    val resultSet: ResultSet = statement.executeQuery()

    // 4、处理查询结果，将没条记录安装(列名,列值)进行封装到List返回
    val colNum: Int = resultSet.getMetaData.getColumnCount
    val resultList = new ListBuffer[mutable.HashMap[String,String]]()
    while (resultSet.next()) {
      val columnValueMap = new mutable.HashMap[String,String]()
      for (i <- 1.to(colNum)) {
        columnValueMap(resultSet.getMetaData.getColumnName(i)) = resultSet.getString(i)
      }
      resultList.append(columnValueMap)
    }
    resultList
  }
}
