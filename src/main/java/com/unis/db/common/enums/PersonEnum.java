package com.unis.db.common.enums;

/**
 * @author xuli
 * @date 2019/5/14
 */
public class PersonEnum {
    /**
     * 人脸卡口 共103个，
     * 例子:50010500001311000xxx
     */
    public static final String DEVICE_PREFIX = "50010500001311000";
    /**
     * 性别代码
     */
    public static final int[] GENDER_TYPE = {0, 1, 2, 9};
    /**
     * 颜色种类
     */
    public static final int[] COLOR = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 99};
    /**
     * 上衣款式
     */
    public static final int[] COAT_STYLE = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 99};
    /**
     * 裤子款式
     */
    public static final int[] TROUSERS_STYLE = {1, 2, 3, 4, 5, 6, 7, 8, 99};
    /**
     * 上衣纹理
     */
    public static final int[] COAT_GRAIN = {0, 1, 2, 3, 4, 99};
    /**
     * 朝向
     */
    public static final int[] ORIENTATION = {0, 1, 2, 3, 4, 5, 99};
}
