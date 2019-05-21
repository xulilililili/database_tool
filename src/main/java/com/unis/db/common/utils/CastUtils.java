package com.unis.db.common.utils;

/**
 * @author xuli
 * @date 2019/5/21
 * 数据转型工具类
 */
public class CastUtils {
    /**
     * 转为String类型
     *
     * @param obj obj
     * @return String
     */
    public static String castString(Object obj) {
        return CastUtils.castString(obj, "");
    }

    /**
     * 转为String类型（提供默认值）
     *
     * @param obj          obj
     * @param defaultValue 默认值
     * @return 如果obj为null则返回default
     */
    public static String castString(Object obj, String defaultValue) {
        return obj != null ? String.valueOf(obj) : defaultValue;
    }

    /**
     * 转为double类型
     *
     * @param obj obj
     * @return 如果为null或者空字符串或者格式不对则返回0
     */
    public static double castDouble(Object obj) {
        return CastUtils.castDouble(obj, 0);
    }

    /**
     * 转为double类型
     *
     * @param obj          obj
     * @param defaultValue 默认值
     * @return double 如果obj为null或者空字符串或者格式不对则返回defaultValue
     */
    public static double castDouble(Object obj, double defaultValue) {
        double value = defaultValue;
        if (obj != null) {
            String strValue = castString(obj);
            //不为空则把值赋给value
            value = Double.parseDouble(strValue);
        }
        return value;
    }

    /**
     * 转为long型
     *
     * @param obj obj
     * @return 如果obj为null或者空字符串或者格式不对则返回0
     */
    public static long castLong(Object obj) {
        return CastUtils.castLong(obj, 0);
    }

    /**
     * 转为long型（提供默认数值）
     *
     * @param obj          obj
     * @param defaultValue 默认值
     * @return obj为null或者空字符串或者格式不对返回defaultValue
     */
    public static long castLong(Object obj, long defaultValue) {
        long value = defaultValue;
        if (obj != null) {
            String strValue = castString(obj);
            //不为空则把值赋给value
            value = Long.parseLong(strValue);
        }
        return value;
    }

    /**
     * 转为int型
     *
     * @param obj obj
     * @return 如果obj为null或者空字符串或者格式不对则返回0
     */
    public static int castInt(Object obj) {
        return CastUtils.castInt(obj, 0);
    }

    /**
     * 转为int型(提供默认值)
     *
     * @param obj          obj
     * @param defaultValue 默认值
     * @return 如果obj为null或者空字符串或者格式不对则返回defaultValue
     */
    public static int castInt(Object obj, int defaultValue) {
        int value = defaultValue;
        if (obj != null) {
            String strValue = castString(obj);
            //不为空则把值赋给value
            value = Integer.parseInt(strValue);
        }
        return value;
    }

}
