package com.ibeifeng.senior.usertrack.spark.util

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkConfUtil {
  /**
    * 根据app名称和是否是本地运行环境返回一个SparkConf对象
    *
    * @param appName app名称
    * @param isLocal 是否是本地运行环境，默认为false
    * @return
    */
  def generateSparkConf(appName: String, isLocal: Boolean = false): SparkConf = {
    val conf = if (isLocal) {
      // 本地运行环境，设置master
      new SparkConf()
        .setAppName(appName)
        .setMaster("local[*]")
    } else {
      // 集群运行环境，不设置master，master参数由启动脚本给定
      new SparkConf()
        .setAppName(appName)
    }

    // 设置一些共同的配置项
    conf.set("spark.sql.shuffle.partitions", "10")
    // RDD进行数据cache的时候，内存最多允许存储的大小（占executor的内存比例），默认0.6
    // 如果内存不够，可能有部分数据不会进行cache(CacheManager会对cache的RDD数据进行管理操作<删除不会用的RDD缓存>)
    conf.set("spark.storage.memoryFraction", "0.6")
    // RDD进行shuffle的时候，shuffle数据写内存的阈值(占executor的内存比例），默认0.2
    conf.set("spark.shuffle.memoryFraction", "0.2")
    // TODO: 如果发现GC频繁而且持续时间长，这两个参数适当调低

    // 参数修改
    // 如果修改的HADOOP配置项，必须在配置项key前一个前缀:"spark.hadoop."
    conf.set("spark.hadoop.mapreduce.input.fileinputformat.split.minsize", "616448000")
    conf.set("spark.streaming.blockInterval", "1s")

    // 返回创建的SparkConf对象
    conf
  }
}

object SparkContextUtil {
  /**
    * 根据给定的SparkConf创建SparkContext对象<br/>
    * 如果JVM中存在一个SparkContext对象，那么直接返回已有的数据，否则创建一个新的SparkContext对象并返回
    *
    * @param conf
    * @return
    */
  def getSparkContext(conf: SparkConf = new SparkConf()): SparkContext = SparkContext.getOrCreate(conf)

}

object SQLContextUtil {
  @transient private var hiveContext: HiveContext = _
  @transient private var sqlContext: SQLContext = _

  /**
    * 根据给定的参数创建一个单例的SQLContext对象，如果#integratedHive参数为true的时候，表示最终返回的SQLContext是需要集成Hive的；如果给定了具体的generateMockData的方法，那么必须调用该方法创建测试数据(默认不创建任何数据)
    *
    * @param sc               SQLContext依赖的SparkContext对象
    * @param integratedHive   是否需要集成hive标识符，true表示需要集成，false表示不需要集成
    * @param generateMockData 创建模拟数据的方法
    * @return 返回一个SQLContext对象
    */
  def getInstance(
                   sc: SparkContext,
                   integratedHive: Boolean = true,
                   generateMockData: (SparkContext, SQLContext) => Unit = (sc: SparkContext, sqlContext: SQLContext) => {}
                 ): SQLContext = {
    // 产生SQLContext对象
    val context = if (integratedHive) {
      if (hiveContext == null) {
        synchronized {
          if (hiveContext == null) {
            hiveContext = new HiveContext(sc)
            generateMockData(sc, hiveContext)
          }
        }
      }

      // 返回创建好的HiveContext
      hiveContext
    } else {
      if (sqlContext == null) {
        synchronized {
          if (sqlContext == null) {
            sqlContext = new SQLContext(sc)
            generateMockData(sc, sqlContext)
          }
        }
      }

      // 返回创建好的SQLContext对象
      sqlContext
    }

    // 返回对象
    context
  }


}