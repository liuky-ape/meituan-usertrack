//package com.ibeifeng.senior.usertrack.spark.util
//
//import com.alibaba.fastjson.{JSONArray, JSONObject}
//import com.ibeifeng.senior.usertrack.util.NumberUtils
//
///**
//  * Created by ibf on 03/07.
//  */
//object JSONUtil {
//  /**
//    * 合并会话聚合结果为json格式的数据
//    * 注意：double数据需要进行格式化操作，不进行科学计数法
//    *
//    * @param totalSessionCount
//    * @param totalSessionLength
//    * @param invalidSessionCount
//    * @param preSessionLengthTypeOfSessionCount
//    * @param preHourOfSessionCount
//    * @param preHourOfSessionLength
//    * @param preHourOfInvalidSessionCount
//    * @return
//    */
//  def mergeSessionAggrResultToJSONString(
//                                          totalSessionCount: Long,
//                                          totalSessionLength: Double,
//                                          invalidSessionCount: Long,
//                                          preSessionLengthTypeOfSessionCount: Array[(String, Int)],
//                                          preHourOfSessionCount: Array[((String, Int), Int)],
//                                          preHourOfSessionLength: Array[((String, Int), Long)],
//                                          preHourOfInvalidSessionCount: Array[((String, Int), Int)]): String = {
//    val tmpArray = new JSONArray()
//    val obj = new JSONObject()
//    // 1. 添加总会话数量
//    obj.put("total_session_count", totalSessionCount)
//    // 2. 添加总会话长度
//    obj.put("total_session_length", NumberUtils.formatDoubleOfNotUseGrouping(totalSessionLength))
//    // 3. 添加无效会话数量
//    obj.put("invalid_session_count", invalidSessionCount)
//    // 4. 添加每种会话类型中的会话数量
//    tmpArray.clear()
//    for ((stype, scount) <- preSessionLengthTypeOfSessionCount) {
//      val obj1 = new JSONObject()
//      obj1.put("type", stype)
//      obj1.put("count", scount)
//      tmpArray.add(obj1)
//    }
//    obj.put("pre_session_type_of_count", tmpArray.clone())
//    // 5. 添加每天每个小时中的会话数量
//    tmpArray.clear()
//    for (((day, hour), count) <- preHourOfSessionCount) {
//      val obj1 = new JSONObject()
//      obj1.put("day", day)
//      obj1.put("hour", hour)
//      obj1.put("count", count)
//      tmpArray.add(obj1)
//    }
//    obj.put("pre_hour_of_session_count", tmpArray.clone())
//    // 6. 添加每天每个小时的会话长度
//    tmpArray.clear()
//    for (((day, hour), length) <- preHourOfSessionLength) {
//      val obj1 = new JSONObject()
//      obj1.put("day", day)
//      obj1.put("hour", hour)
//      obj1.put("length", length)
//      tmpArray.add(obj1)
//    }
//    obj.put("pre_hour_of_session_length", tmpArray.clone())
//    // 7. 添加每天每个小时的无效会话数量
//    tmpArray.clear()
//    for (((day, hour), invalid) <- preHourOfInvalidSessionCount) {
//      val obj1 = new JSONObject()
//      obj1.put("day", day)
//      obj1.put("hour", hour)
//      obj1.put("invalid", invalid)
//      tmpArray.add(obj1)
//    }
//    obj.put("pre_hour_of_invalid_session_count", tmpArray.clone())
//
//    // 返回json数据
//    obj.toJSONString
//  }
//
//  /**
//    * 合并抽样的session数据为json格式
//    *
//    * @param day
//    * @param hour
//    * @param record
//    * @return
//    */
//  def mergeSampleSessionResultToJSONString(day: String, hour: Int, record: UserVisitSessionRecord): String = {
//    val obj = new JSONObject()
//    // 添加普通数据
//    obj.put("session_id", record.sessionId)
//    obj.put("day", day)
//    obj.put("hour", hour)
//
//    // 添加操作记录数据
//    val recordObj = record.transform2JSONObject()
//    obj.put("record", recordObj)
//
//    // 返回结果
//    obj.toJSONString
//  }
//
//
//}
