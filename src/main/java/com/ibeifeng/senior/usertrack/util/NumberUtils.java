package com.ibeifeng.senior.usertrack.util;

import java.math.BigDecimal;
import java.text.NumberFormat;

/**
 * 数字格工具类
 */
public class NumberUtils {

    /**
     * 格式化小数
     *
     * @param num 字符串
     * @return 格式化小数
     */
    public static double formatDouble(double num) {
        return formatDouble(num, 2);
    }

    /**
     * 格式化小数
     *
     * @param num   字符串
     * @param scale 四舍五入的位数
     * @return 格式化小数
     */
    public static double formatDouble(double num, int scale) {
        BigDecimal bd = new BigDecimal(num);
        return bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    /**
     * 格式化数据，不使用科学计数法进行数据显示操作
     *
     * @param num
     * @return
     */
    public static String formatDoubleOfNotUseGrouping(double num) {
        Double value = Double.valueOf(num);
        NumberFormat nf = NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        return nf.format(value);
    }
}
