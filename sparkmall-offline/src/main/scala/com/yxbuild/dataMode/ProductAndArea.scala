package com.yxbuild.dataMode

/**
  *
  * @param product_id
  * @param product_name
  * @param city_id
  * @param city_name
  * @param area
  */
case class ProductAndArea (
                            product_id: Long,
                            product_name: String,
                            city_id:Long,
                            city_name:String,
                            area:String
                          )
