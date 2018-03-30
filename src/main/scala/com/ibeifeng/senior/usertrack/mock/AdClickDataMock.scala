package com.ibeifeng.senior.usertrack.mock

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * 模拟产生广告点击数据
  * Created by ibf on 03/16.
  */
object AdClickDataMock {
  // 分割字符串
  val delimeter = " "
  val topicName = "AdRealTimeLogSparkProject"


  def main(args: Array[String]): Unit = {
    val running = new AtomicBoolean(true)
    val brokerList = "192.168.187.146:9092,192.168.187.146:9093,192.168.187.146:9094,192.168.187.146:9095"
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config: ProducerConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    for (i <- 0 until 2) {
      new Thread(new Runnable {
        override def run(): Unit = {
          // 数据随机器
          val random = Random

          while (running.get()) {
            // 1. 随机数据
            val messages = generateMessage(random)
            // 2. 发送
            for (message <- messages) {
              producer.send(message)
            }
            // 3. 休息一下
            Thread.sleep(random.nextInt(200) + 50)
          }
        }
      }).start()
    }

    // 运行2min后关闭
    Thread.sleep(3 * 60 * 60 * 1000)
    running.set(false)
    producer.close
  }

  /**
    * 产生一个随机的数据
    *
    * @return
    */
  def generateMessage(random: Random): List[KeyedMessage[String, String]] = {
    val key = random.nextInt(100).toString
    // 0-999
    val cityId = random.nextInt(1000)
    val province = s"province_${cityId % 100}"
    val city = s"city_${cityId}"
    val userId = random.nextInt(10000000)
    val adId = random.nextInt(10000)
    val str = s"${province}${delimeter}${city}${delimeter}${userId}${delimeter}${adId}"

    // 0.05的几率产生一次产生多条数据, num >= 1
    val numbers = if (random.nextDouble() <= 0.05) {
      random.nextInt(500) + 1
    } else {
      1
    }

    val msgs = (0 until numbers).foldLeft(ArrayBuffer[KeyedMessage[String, String]]())((buf, b) => {
      val timestamp = System.currentTimeMillis()
      val value = s"${timestamp}${delimeter}${str}"
      val msg = KeyedMessage[String, String](topicName, key, key, value)

      buf += msg
      buf
    })

    //返回对象
    msgs.toList
  }
}
