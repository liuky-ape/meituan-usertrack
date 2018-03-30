package com.ibeifeng.senior.usertrack.test;

import java.io.IOException;

/**
 * Created by ibf on 12/13.
 */
public class SparkApplicationSubmitTest {
    public static void main(String[] args) {
        long taskId = 1L;

        submitSparkApplication(taskId);
    }

    /**
     * 启动taskid对应的spark应用
     * @param taskId
     */
    public static void submitSparkApplication(long taskId) {
        // 1. 从数据库/缓存中获取该taskid对应的task类型，并获取类型对应的class名称
        String clz = "xxxxxxx";
        // 2. 构建执行命令
        // shell所在的地址路径
        String shellPath = "/opt/module/shell/sparkSubmit.sh";
        // shell脚本需要传递的参数
        String argParam = clz + " " + taskId;
        // 最终执行的命令
        String cmd = "sh " + shellPath + " " + argParam;

        try {
            // 执行命令，获取执行命令的进程对象
            Process process = Runtime.getRuntime().exec(cmd);
            // 等等进程执行完成
            process.waitFor();

            // 获取进程执行的结果值
            int exitValue = process.exitValue();
            if (exitValue == 0) {
                // 脚本执行正常
                // TODO: 更新任务的一些状态信息
            } else {
                // 脚步执行失败
                // TODO: 通知开发人员，执行失败
            }
        } catch (IOException e) {
            // 脚步执行失败
            // TODO: 通知开发人员，执行失败
            e.printStackTrace();
        } catch (InterruptedException e) {
            // 脚步执行失败
            // TODO: 通知开发人员，执行失败
            e.printStackTrace();
        }
    }
}
