package com.yxbuild.dataMode

/**
  * 用户访问动作样例类
  *
  * @param date 用户点击行为的日期
  * @param user_id 用户表示
  * @param session_id 用户访问的Session标识
  * @param page_id 用户访问的页面ID
  * @param action_time 用户操作的时间
  * @param search_keyword 用户搜索的关键词
  * @param click_category_id 用户点击商品分类的标识
  * @param click_product_id 用户点击商品的标识
  * @param order_category_ids 订单分类的标识(多个)
  * @param order_product_ids 订单商品的标识(多个)
  * @param pay_category_ids 支付商品分类的标识(多个)
  * @param pay_product_ids 支付商品的标识(多个)
  * @param city_id 支付所在的城市
  */
case class UserVisitAction(
                     date:String,
                     user_id:Long,
                     session_id:String,
                     page_id:Long,
                     action_time:String,
                     search_keyword:String,
                     click_category_id:Long,
                     click_product_id:Long,
                     order_category_ids:String,
                     order_product_ids:String,
                     pay_category_ids:String,
                     pay_product_ids:String,
                     city_id:Long
                     )
