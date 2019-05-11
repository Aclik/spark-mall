package com.yxbuild.dataMode

/**
  * 用户信息表
  * @param userId 用户的标识
  * @param userName 用户的名称
  * @param name 用户真实姓名
  * @param age 用户的年龄
  * @param professional 用户的职业
  * @param gender 用户的性别
  */
case class UserInfo (
                    userId:Long,
                    userName:String,
                    name: String,
                    age:Int,
                    professional:String,
                    gender:String
                    )
