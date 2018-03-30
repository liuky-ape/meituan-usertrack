package com.ibeifeng.senior.usertrack.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.senior.usertrack.domain.Task;
import scala.Option;

/**
 * Created by XuanYu on 2016/6/4.
 */
public class ParamUtils {

    /**
     * 从命令行参数中提取任务id
     *
     * @param args 命令行参数
     * @return 任务id
     */
    public static Long getTaskIdFromArgs(String[] args) {
        try {
            if (args != null && args.length > 0) {
                return Long.valueOf(args[0]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 依据Task获取参数
     *
     * @param task
     * @return
     */
    public static JSONObject getTaskParam(Task task) {
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        if (taskParam != null && !taskParam.isEmpty()) {
            return taskParam;
        }
        return null;
    }

    /**
     * 从JSON对象中提取参数
     *
     * @param jsonObject JSON对象
     * @return 参数
     */
    public static Option<String> getParam(JSONObject jsonObject, String field) {
        String value = jsonObject.getString(field);
        if (value == null || value.trim().isEmpty()) {
            return Option.apply(null);
        } else {
            return Option.apply(value);
        }
    }
}
