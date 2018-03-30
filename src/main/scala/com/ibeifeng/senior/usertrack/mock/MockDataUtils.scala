package com.ibeifeng.senior.usertrack.mock

import java.util.concurrent.ThreadLocalRandom
import java.util.UUID

import com.alibaba.fastjson.JSONObject
import com.ibeifeng.senior.usertrack.constant.Constants
import com.ibeifeng.senior.usertrack.util.DateUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ArrayBuffer

/**
  * 模拟测试数据
  */
object MockDataUtils {
  val random = ThreadLocalRandom.current()
  val splitSymbol = ","
  // 用户数量
  val userNumbers = 1000
  // 用户每天访问的会话数量上限(100万)
  val userVisitSessionNumbers = 1000000
  // 商品数量（100万）
  val productNumbers = 1000000
  // 商品品类数量(50个)
  val productCategoryNumbers = 50
  // 职业
  val professionals = Array("程序员", "教师", "厨师", "司机", "医生", "护士", "设计师", "农民", "工人", "助理")
  val professionalTypeSize = professionals.length
  // 城市列表,格式为:(0,上海),(1,北京)....
  val citys = Array("上海", "北京", "深圳", "广州", "南京", "杭州", "长沙", "南昌", "张家界", "香港", "澳门").zipWithIndex.map(_.swap)
  val cityTypeSize = citys.length
  // 性别列表
  val sexs = Array("male", "female", "unknown")
  val sexTypeSize = sexs.length
  // 搜索关键词
  val keywords = Array("火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
    "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
  val keywordSize = keywords.length

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("mock-data")
    val sc = SparkContext.getOrCreate(conf)
    // ===========================
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val userInfoDataSavePath = "data/mock/user_info"
    val userVisitActionSavePath = "data/mock/user_visit_action"
    fs.delete(new Path(userInfoDataSavePath), true)
    fs.delete(new Path(userVisitActionSavePath), true)

    // =========================
    val userInfoRDD = mockUserInfo(sc)
    userInfoRDD.map(_.formatted(splitSymbol)).saveAsTextFile(userInfoDataSavePath)

    val userVisitActionRDD = mockUserVisitAction(sc)
    userVisitActionRDD.map(_.formatted(splitSymbol)).saveAsTextFile(userVisitActionSavePath)
  }

  /**
    * 产生用户信息的模拟代码
    *
    * @param sc
    * @return
    */
  def mockUserInfo(sc: SparkContext): RDD[UserInfo] = {
    val buffer = ArrayBuffer[UserInfo]()
    for (i <- 0 until userNumbers) {
      val userId: Long = i.toLong
      val userName: String = s"user_${i}"
      val name: String = s"name_${i}"
      val age: Int = random.nextInt(15, 60)
      val professional: String = professionals(random.nextInt(professionalTypeSize))
      val city: String = citys(random.nextInt(cityTypeSize))._2
      val sex: String = sexs(random.nextInt(sexTypeSize))

      buffer += UserInfo(
        userId, userName, name,
        age, professional, city, sex)
    }

    // 构建RDD并返回
    sc.parallelize(buffer)
  }

  /**
    * 模拟产生UserVisitAction的访问数据
    *
    * @param sc
    * @return
    */
  def mockUserVisitAction(sc: SparkContext): RDD[UserVisitAction] = {
    val date: String = DateUtils.getTodayDate()
    val buffer = ArrayBuffer[UserVisitAction]()

    // 根据上一个操作的时间获取当前操作的时间
    def getCurrentActionTime(preActionTime: Long): Long = {
      // 90%，增加1分钟以内，10%增加1-5分钟
      val (least, bound) = if (random.nextDouble(1) <= 0.9) {
        (1, 60000)
      } else {
        (60000, 300000)
      }
      preActionTime + random.nextInt(least, bound)
    }

    // 产生一个商品id和商品品类
    def generateProduceAndCategoryId(): (Long, Long) = {
      val produceID = random.nextLong(productNumbers)
      (produceID, produceID % productCategoryNumbers)
    }

    // 产生pageView数据
    def generatePageView(times: Int, userId: Long, sessionId: String, cityId: Int, preActionTime: Long): Unit = {
      if (times < 20) {
        // 添加新数据
        // pageView的页面ID范围是:[0,100)
        val pageId: Long = random.nextInt(100)
        val currentActionTime: Long = getCurrentActionTime(preActionTime)
        val actionTime: String = DateUtils.parseLong2String(currentActionTime)
        val searchKeyword: String = ""
        val clickCategoryId: String = ""
        val clickProductId: String = ""
        val orderCategoryIds: String = ""
        val orderProductIds: String = ""
        val payCategoryIds: String = ""
        val payProductIds: String = ""

        // 添加数据
        buffer += new UserVisitAction(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId)

        // 进入下一步操作
        /**
          * 浏览之后可能存在搜索、点击和继续浏览三种情况， 也存在直接退出的情况
          * 当times次数小于3的时候，45%继续浏览，25%搜索，20%的点击，10%直接退出
          * 当times次数[3,10)的时候，10%浏览，25%搜素，35%的点击，30%直接退出
          * 当times次数[10,20)的时候，5%的浏览，5%的搜索，10%的点击，80%直接退出
          **/
        val randomValue = random.nextDouble(1)
        val (t1, t2, t3) =
          if (times < 3) {
            (0.45, 0.7, 0.9)
          } else if (times < 10) {
            (0.1, 0.35, 0.7)
          } else {
            (0.05, 0.1, 0.2)
          }


        if (randomValue <= t1) {
          // 浏览
          generatePageView(times + 1, userId, sessionId, cityId, currentActionTime)
        } else if (randomValue <= t2) {
          // 搜索
          generateSearch(times + 1, userId, sessionId, cityId, currentActionTime)
        } else if (randomValue <= t3) {
          // 点击
          generateClick(times + 1, userId, sessionId, cityId, currentActionTime)
        } else {
          // nothings, 结束
        }
      }
    }

    // 产生搜索数据
    def generateSearch(times: Int, userId: Long, sessionId: String, cityId: Int, preActionTime: Long): Unit = {
      if (times < 20) {
        // 添加新数据
        // search的页面ID范围是:[100,150)
        val pageId: Long = random.nextInt(100, 150)
        val currentActionTime = getCurrentActionTime(preActionTime)
        val actionTime: String = DateUtils.parseLong2String(currentActionTime)
        val searchKeyword: String = keywords(random.nextInt(keywordSize))
        val clickCategoryId: String = ""
        val clickProductId: String = ""
        val orderCategoryIds: String = ""
        val orderProductIds: String = ""
        val payCategoryIds: String = ""
        val payProductIds: String = ""

        // 添加数据
        buffer += new UserVisitAction(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId)

        // 进入下一步操作
        /**
          * 搜索之后可能存在点击、浏览和继续搜索三种情况， 也存在直接退出的情况
          * 当times次数小于3的时候，20%浏览，25%搜索，45%的点击，10%直接退出
          * 当times次数[3,10)的时候，10%浏览，10%搜素，30%的点击，50%直接退出
          * 当times次数[10,20)的时候，1%的浏览，1%的搜索，8%的点击，90%直接退出
          **/
        val randomValue = random.nextDouble(1)
        val (t1, t2, t3) =
          if (times < 3) {
            (0.2, 0.45, 0.9)
          } else if (times < 10) {
            (0.1, 0.2, 0.5)
          } else {
            (0.01, 0.02, 0.1)
          }


        if (randomValue <= t1) {
          // 浏览
          generatePageView(times + 1, userId, sessionId, cityId, currentActionTime)
        } else if (randomValue <= t2) {
          // 搜索
          generateSearch(times + 1, userId, sessionId, cityId, currentActionTime)
        } else if (randomValue <= t3) {
          // 点击
          generateClick(times + 1, userId, sessionId, cityId, currentActionTime)
        } else {
          // nothings, 结束
        }
      }
    }

    // 产生点击事件数据
    def generateClick(times: Int, userId: Long, sessionId: String, cityId: Int, preActionTime: Long): Unit = {
      if (times < 20) {
        // click的页面ID范围是:[150,300)
        val pageId: Long = random.nextInt(150, 300)
        val currentActionTime = getCurrentActionTime(preActionTime)
        val actionTime: String = DateUtils.parseLong2String(currentActionTime)
        val searchKeyword: String = ""
        val (productID, categoryID) = generateProduceAndCategoryId()
        val clickProductId: String = productID.toString
        val clickCategoryId: String = categoryID.toString
        val orderCategoryIds: String = ""
        val orderProductIds: String = ""
        val payCategoryIds: String = ""
        val payProductIds: String = ""

        // 添加数据
        buffer += new UserVisitAction(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId)

        // 进入下一步操作
        /**
          * 点击之后可能存在浏览、搜索、下单和继续点击四种情况， 也存在直接退出的情况
          * 当times次数小于3的时候，10%继续浏览，10%搜索，50%下单，25%的点击，5%直接退出
          * 当times次数[3,10)的时候，5%继续浏览，5%搜索，45%下单，20%的点击，25%直接退出
          * 当times次数[10,20)的时候，1%继续浏览，1%搜索，30%下单，8%的点击，60%直接退出
          **/
        val randomValue = random.nextDouble(1)
        val (t1, t2, t3, t4) =
          if (times < 3) {
            (0.1, 0.2, 0.7, 0.95)
          } else if (times < 10) {
            (0.05, 0.1, 0.55, 0.75)
          } else {
            (0.01, 0.02, 0.32, 0.4)
          }


        if (randomValue <= t1) {
          // 浏览
          generatePageView(times + 1, userId, sessionId, cityId, currentActionTime)
        } else if (randomValue <= t2) {
          // 搜索
          generateSearch(times + 1, userId, sessionId, cityId, currentActionTime)
        } else if (randomValue <= t3) {
          // 下单
          generateOrder(times + 1, userId, sessionId, cityId, currentActionTime)
        } else if (randomValue <= t4) {
          // 点击
          generateClick(times + 1, userId, sessionId, cityId, currentActionTime)
        } else {
          // nothings, 结束
        }
      }
    }

    // 产生订单数据
    def generateOrder(times: Int, userId: Long, sessionId: String, cityId: Int, preActionTime: Long): Unit = {
      if (times < 20) {
        // order的页面ID范围是:[300,301)
        val pageId: Long = 300
        val currentActionTime = getCurrentActionTime(preActionTime)
        val actionTime: String = DateUtils.parseLong2String(currentActionTime)
        val searchKeyword: String = ""
        val clickProductId: String = ""
        val clickCategoryId: String = ""
        // 可能存在多个商品或者品类在一起下单,数量范围:[1,6)
        val randomProductNumbers = random.nextInt(1, 6)
        val bf = ArrayBuffer[(Long, Long)]()
        for (j <- 0 until randomProductNumbers) {
          bf += generateProduceAndCategoryId()
        }
        // 去掉重复数据
        val nbf = bf.distinct

        val orderCategoryIds: String = nbf.map(_._2).mkString(Constants.SPLIT_CATEGORY_OR_PRODUCT_ID_SEPARATOR)
        val orderProductIds: String = nbf.map(_._1).mkString(Constants.SPLIT_CATEGORY_OR_PRODUCT_ID_SEPARATOR)
        val payCategoryIds: String = ""
        val payProductIds: String = ""

        // 添加数据
        buffer += new UserVisitAction(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId)

        // 进入下一步操作
        /**
          * 下单之后可能存在搜索、浏览和支付三种情况， 也存在直接退出的情况
          * 当times次数小于等于3的时候，5%继续浏览，5%搜索，90%支付，0%直接退出
          * 当times次数(3,10)的时候，5%继续浏览，5%搜索，80%支付，10%直接退出
          * 当times次数[10,20)的时候，1%继续浏览，1%搜索，70%支付，28%直接退出
          **/
        val randomValue = random.nextDouble(1)
        val (t1, t2, t3) =
          if (times <= 3) {
            (0.05, 0.1, 1.0)
          } else if (times < 10) {
            (0.05, 0.1, 0.9)
          } else {
            (0.01, 0.02, 0.72)
          }


        if (randomValue <= t1) {
          // 浏览
          generatePageView(times + 1, userId, sessionId, cityId, currentActionTime)
        } else if (randomValue <= t2) {
          // 搜索
          generateSearch(times + 1, userId, sessionId, cityId, currentActionTime)
        } else if (randomValue <= t3) {
          // 支付
          generatePay(times + 1, userId, sessionId, cityId, currentActionTime, productIds = orderProductIds, categoryIds = orderCategoryIds)
        } else {
          // nothings, 结束
        }
      }
    }

    // 产生支付数据
    def generatePay(times: Int, userId: Long, sessionId: String, cityId: Int, preActionTime: Long, productIds: String, categoryIds: String): Unit = {
      if (times <= 20) {
        // pay的页面ID范围是:301
        val pageId: Long = 301
        val currentActionTime = getCurrentActionTime(preActionTime)
        val actionTime: String = DateUtils.parseLong2String(currentActionTime)
        val searchKeyword: String = ""
        val clickProductId: String = ""
        val clickCategoryId: String = ""
        val orderCategoryIds: String = ""
        val orderProductIds: String = ""
        val payCategoryIds: String = categoryIds
        val payProductIds: String = productIds

        // 添加数据
        buffer += new UserVisitAction(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId)

        // 进入下一步操作
        /**
          * 支付之后可能存在搜索和浏览两种情况， 也存在直接退出的情况
          * 当times次数[0,10)的时候，45%继续浏览，45%搜索，10%直接退出
          * 当times次数[10,20)的时候，20%继续浏览，20%搜索，60%直接退出
          **/
        val randomValue = random.nextDouble(1)
        val (t1, t2) =
          if (times < 10) {
            (0.45, 0.9)
          } else {
            (0.2, 0.4)
          }


        if (randomValue <= t1) {
          // 浏览
          generatePageView(times + 1, userId, sessionId, cityId, currentActionTime)
        } else if (randomValue <= t2) {
          // 搜索
          generateSearch(times + 1, userId, sessionId, cityId, currentActionTime)
        } else {
          // nothings, 结束
        }
      }
    }


    //    val sessionNumbers = random.nextInt(userVisitSessionNumbers / 10, userVisitSessionNumbers)
    val sessionNumbers = userVisitSessionNumbers
    for (i <- 0 until sessionNumbers) {
      val startTime = DateUtils.getRandomTodayTimeOfMillis(random)
      val userId: Long = random.nextLong(userNumbers)
      val sessionId: String = UUID.randomUUID().toString
      val cityId: Int = citys(random.nextInt(cityTypeSize))._1
      // action主要分为：浏览、搜索、点击、下单及支付
      /**
        * 假设访问链有以下情况：
        * 1. 浏览 -> 搜索 -> 点击 -> 下单 -> 支付
        * 2. 搜索 -> 点击 -> 下单 -> 支付
        * 3. 浏览 -> 点击 -> 下单 -> 支付
        * 注意：其中浏览、搜索、点击可能连续出现多次，但是下单和支付不会存在连续出现的情况
        * 当一个流程执行到下单或者支付的时候，可能会再次重新开始执行
        * 假设一个会话中的事件触发次数最多不超过20次
        * 每两个事件之间的时间间隔不超过5分钟，最少1秒钟
        * ======>
        * 浏览之后可能存在搜索、点击和继续浏览三种情况
        * 搜索之后可能存在点击、浏览和继续搜索三种情况
        * 点击之后可能存在浏览、搜索、下单和继续点击四种情况
        * 下单之后可能存在搜索、浏览和支付三种情况
        * 支付之后可能存在搜索和浏览两种情况
        * 备注：所有事件之后都可能存在结束的操作
        **/
      // 80%的几率进入浏览，20%直接进入搜索
      if (random.nextDouble(1) <= 0.8) {
        // 产生一个浏览数据
        generatePageView(0, userId, sessionId, cityId, startTime)
      } else {
        // 产生一个搜索数据
        generateSearch(0, userId, sessionId, cityId, startTime)
      }
    }

    // 返回rdd
    sc.parallelize(buffer)
  }

  /**
    * 模拟产生商品数据
    *
    * @param sc
    * @return
    */
  def mockProductInfo(sc: SparkContext): RDD[ProductInfo] = {
    // 1. 创建Data数据
    val buffer = ArrayBuffer[ProductInfo]()
    for (i <- 0 until productNumbers) {
      val productID: Long = i.toLong
      val productName: String = s"product_${productID}"
      // 60%是第三方商品 40%是自营商品
      val extendInfo: String = {
        val obj = new JSONObject()
        if (random.nextDouble(1) <= 0.4) {
          // 自营商品
          obj.put("product_type", "0")
        } else {
          // 第三方商品
          obj.put("product_type", "1")
        }
        obj.toJSONString
      }

      buffer += new ProductInfo(productID, productName, extendInfo)
    }

    // 2. 创建RDD
    val rdd = sc.parallelize(buffer)

    // 3. RDD返回
    rdd
  }

  /**
    * 加载商品模拟信息
    *
    * @param sc
    * @param sqlContext
    */
  def loadProductInfoMockData(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val productInfoDataSavePathStr = "data/mock/product_info"
    val productInfoDataSavePath = new Path(productInfoDataSavePathStr)

    val productInfoRDD: RDD[ProductInfo] = {
      if (!fs.exists(productInfoDataSavePath)) {
        // 数据不存在，创建并返回rdd
        val rdd = mockProductInfo(sc)
        rdd.map(_.formatted()).saveAsTextFile(productInfoDataSavePathStr)
        // 返回
        rdd
      } else {
        // 读取数据
        val rdd = sc.textFile(productInfoDataSavePathStr)
        // rdd转换并返回
        rdd.map(line => ProductInfo.parseProductInfo(line))
          .filter(_.isDefined)
          .map(_.get)
      }
    }

    // 转换为DF并注册成为表
    import sqlContext.implicits._
    productInfoRDD
      .toDF(ProductInfo.columnNames: _*)
      .registerTempTable("product_info")
  }

  /**
    * 产生模拟数据
    *
    * @param sc
    * @param sqlContext
    */
  def mockData(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val userInfoDataSavePathStr = "data/mock/user_info"
    val userInfoDataSavePath = new Path(userInfoDataSavePathStr)
    val userVisitActionSavePathStr = "data/mock/user_visit_action"
    val userVisitActionSavePath = new Path(userVisitActionSavePathStr)

    if (!fs.exists(userInfoDataSavePath)) {
      // 数据不存在
      val userInfoRDD = mockUserInfo(sc)
      userInfoRDD.map(_.formatted(splitSymbol)).saveAsTextFile(userInfoDataSavePathStr)
      // 并返回rdd
    }

    if (!fs.exists(userVisitActionSavePath)) {
      // 数据不存在
      val userVisitActionRDD = mockUserVisitAction(sc)
      userVisitActionRDD.map(_.formatted(splitSymbol)).saveAsTextFile(userVisitActionSavePathStr)
    }

    // 开始读取数据形成DataFrame
    val userInfoRDD = sc.textFile(userInfoDataSavePathStr)
    val userVisitActionRDD = sc.textFile(userVisitActionSavePathStr)
    // 开始转换为DataFrame并注册成为表
    import sqlContext.implicits._
    userInfoRDD
      .map(line => UserInfo.parseUserInfo(line))
      .filter(_.isDefined)
      .map(_.get)
      .toDF(UserInfo.columnNames: _*)
      .registerTempTable("user_info")
    userVisitActionRDD
      .map(line => UserVisitAction.parseUserVisitAction(line))
      .filter(_.isDefined)
      .map(_.get)
      .toDF(UserVisitAction.columnNames: _*)
      .registerTempTable("user_visit_action")
  }
}

case class ProductInfo(
                        productID: Long,
                        productName: String,
                        extendInfo: String
                      ) {
  /**
    * 格式化
    *
    * @param splitSymbol
    * @return
    */
  def formatted(splitSymbol: String = "^"): String = {
    s"${productID}${splitSymbol}${productName}${splitSymbol}${extendInfo}"
  }
}

object ProductInfo {
  /**
    * 表列名称
    */
  val columnNames = Array("product_id", "product_name", "extend_info")

  /**
    * 解析行数据，并返回对象；如果解析失败返回None
    *
    * @param line
    * @param splitSymbol
    * @return
    */
  def parseProductInfo(line: String, splitSymbol: String = "\\^"): Option[ProductInfo] = {
    val arr = line.split(splitSymbol)
    if (arr.length == 3) {
      Some(
        new ProductInfo(
          arr(0).toLong,
          arr(1),
          arr(2)
        )
      )
    } else None
  }
}

case class UserVisitAction(
                            date: String,
                            userId: Long,
                            sessionId: String,
                            pageId: Long,
                            actionTime: String,
                            searchKeyword: String,
                            clickCategoryId: String,
                            clickProductId: String,
                            orderCategoryIds: String,
                            orderProductIds: String,
                            payCategoryIds: String,
                            payProductIds: String,
                            cityId: Int
                          ) {
  /**
    * 格式化时间
    *
    * @param splitSymbol
    * @return
    */
  def formatted(splitSymbol: String = ","): String = {
    s"${date}${splitSymbol}${userId}${splitSymbol}${sessionId}${splitSymbol}${pageId}${splitSymbol}${actionTime}${splitSymbol}${searchKeyword}${splitSymbol}${clickCategoryId}${splitSymbol}${clickProductId}${splitSymbol}${orderCategoryIds}${splitSymbol}${orderProductIds}${splitSymbol}${payCategoryIds}${splitSymbol}${payProductIds}${splitSymbol}${cityId}"
  }
}

object UserVisitAction {
  /**
    * 表列名称
    */
  val columnNames = Array("date", "user_id", "session_id", "page_id", "action_time", "search_keyword", "click_category_id", "click_product_id", "order_category_ids", "order_product_ids", "pay_category_ids", "pay_product_ids", "city_id")

  /**
    * 解析行数据，并返回对象；如果解析失败返回None
    *
    * @param line
    * @param splitSymbol
    * @return
    */
  def parseUserVisitAction(line: String, splitSymbol: String = ","): Option[UserVisitAction] = {
    val arr = line.split(splitSymbol)
    if (arr.length == 13) {
      Some(
        new UserVisitAction(
          arr(0),
          arr(1).toLong,
          arr(2),
          arr(3).toLong,
          arr(4),
          arr(5),
          arr(6),
          arr(7),
          arr(8),
          arr(9),
          arr(10),
          arr(11),
          arr(12).toInt
        )
      )
    } else None
  }
}


case class UserInfo(
                     userId: Long,
                     userName: String,
                     name: String,
                     age: Int,
                     professional: String,
                     city: String,
                     sex: String
                   ) {
  /**
    * 格式化时间
    *
    * @param splitSymbol
    * @return
    */
  def formatted(splitSymbol: String = ","): String = {
    s"${userId}${splitSymbol}${userName}${splitSymbol}${name}${splitSymbol}${age}${splitSymbol}${professional}${splitSymbol}${city}${splitSymbol}${sex}"
  }
}

object UserInfo {
  /**
    * 表列明
    */
  val columnNames = Array("user_id", "user_name", "name", "age", "professional", "city", "sex")

  /**
    * 解析行数据，并返回对象；如果解析失败返回None
    *
    * @param line
    * @param splitSymbol
    * @return
    */
  def parseUserInfo(line: String, splitSymbol: String = ","): Option[UserInfo] = {
    val arr = line.split(splitSymbol)
    if (arr.length == 7) {
      Some(new UserInfo(
        arr(0).toLong,
        arr(1),
        arr(2),
        arr(3).toInt,
        arr(4),
        arr(5),
        arr(6)
      ))
    } else None
  }
}