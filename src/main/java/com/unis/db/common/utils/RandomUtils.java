package com.unis.db.common.utils;

import com.unis.db.common.enums.VehicleEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;

/**
 * @author xuli
 * @date 2019/2/12
 */
public class RandomUtils {
    private static final Logger logger = LoggerFactory.getLogger(RandomUtils.class);

    /**
     * 创建recordID相关参数
     */
    private static final int PASS_TIME_DIGIT = 32;
    private static final long RANDOM_DIGIT = 17;
    private static final long NODE_DIGIT = 12;
    private static final long PASS_TIME_GET = ((1L << PASS_TIME_DIGIT) - 1) << RANDOM_DIGIT;

    private static Random random = new Random();

    /**
     * front add 0
     *
     * @param powNum 几次幂
     * @param length 长度
     * @return 补零后的数
     */
    private static String frontAddZero(int powNum, long length) {
        StringBuilder str = new StringBuilder(Integer.toBinaryString(powNum));
        while (str.length() < length) {
            str.append("0");
        }
        return str.toString();
    }

    /**
     * create standard recordID
     *
     * @param passTime 时间戳
     * @return recordID
     */
    public static long getRandomRecordID(long passTime) {
        String node12 = frontAddZero(random.nextInt((int) Math.pow(2, NODE_DIGIT)), NODE_DIGIT);
        String random17 = frontAddZero(random.nextInt((int) Math.pow(2, RANDOM_DIGIT)), RANDOM_DIGIT);
        String service3 = String.format("%03d", Integer.parseInt(Integer.toBinaryString(random.nextInt(2))));
        String passTime32 = String.format("%0" + (32 - Long.toBinaryString(passTime / 1000).length()) + "d", 0)
                + Long.toBinaryString(passTime / 1000);
        //二进制转长整型(Binary -> Long)
        return Long.parseLong(service3 + node12 + passTime32 + random17, 2);
    }

    /**
     * get passTime from recordID
     *
     * @param recordID recordID
     * @return passTime
     */
    public static long getPassTime(long recordID) {
        return (recordID & PASS_TIME_GET) >> RANDOM_DIGIT;
    }

    /**
     * create random number between begin and end
     *
     * @param begin 起始时间戳
     * @param end   结束时间戳
     * @return 中间随机时间戳
     */
    private static long getRandomLong(long begin, long end) {
        long rtn = begin + (long) (Math.random() * (end - begin));
        if (rtn == begin || rtn == end) {
            return getRandomLong(begin, end);
        } else {
            return rtn;
        }
    }

    /**
     * create random long(passTime) between begin and end
     *
     * @param begin 起始时间 类型：yyyyMMdd
     * @param end   结束时间 类型：yyyyMMdd
     * @return 中间随机时间戳
     */
    public static Long getRandomPassTime(String begin, String end) {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        try {
            Long beginDate = format.parse(begin).getTime();
            Long endDate = format.parse(end).getTime();
            if (beginDate >= endDate) {
                return 0L;
            } else {
                return getRandomLong(beginDate, endDate);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0L;
    }

    /**
     * 随机生成某个月内的时间戳
     *
     * @param yearMonth 月份
     * @return 使劲戳
     */
    public static Long getRandomPassTimeInMonth(String yearMonth) {
        String begin = yearMonth + "01";
        return getRandomPassTime(begin, DateUtils.getDateByAdd(begin, DateUtils.getMaxDate(yearMonth)));
    }

    /**
     * 得到某一个月份的随机一天， 且这一天+days 小于当月最大天数
     *
     * @param month 月份
     * @param days  天数
     * @return 随机天数
     */
    public static String getRandomDateInMonth(String month, int days) {
        DecimalFormat df = new DecimalFormat("00");
        String startDate = month + "01";
        if (days != DateUtils.getMaxDate(month)) {
            startDate = month + df.format(random.nextInt(DateUtils.getMaxDate(month) - days) + 1);
        }
        return startDate;
    }

    /**
     * 需要设置50%的车牌开头是浙
     *
     * @return 车牌号码
     */
    public static String getRandomPlateNo() {
        int num = random.nextInt(40);
        String province = "浙";
        if (num < VehicleEnum.VEHICLE_PROVINCE.length) {
            province = VehicleEnum.VEHICLE_PROVINCE[num];
        }
        return province +
                VehicleEnum.VEHICLE_SECOND[random.nextInt(VehicleEnum.VEHICLE_SECOND.length)] +
                VehicleEnum.VEHICLE_NUM[random.nextInt(VehicleEnum.VEHICLE_NUM.length)] +
                VehicleEnum.VEHICLE_RANDOM[random.nextInt(VehicleEnum.VEHICLE_RANDOM.length)] +
                VehicleEnum.VEHICLE_SMALL_NUM1[random.nextInt(VehicleEnum.VEHICLE_SMALL_NUM1.length)] +
                VehicleEnum.VEHICLE_SECOND[random.nextInt(VehicleEnum.VEHICLE_SECOND.length)] +
                VehicleEnum.VEHICLE_NUM[random.nextInt(VehicleEnum.VEHICLE_NUM.length)];
    }

    /**
     * 随机创建id连续的
     *
     * @param number 设备id数量
     * @return 拼接后的设备id
     */
    public static String getRandomDeviceIds(int number) {
        int startNum = random.nextInt(VehicleEnum.DEVICE_SUFFIX.length - number);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < number; i++) {
            sb.append("'").append(VehicleEnum.DEVICE_PREFIX).append(VehicleEnum.DEVICE_SUFFIX[startNum++]).append("',");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    /**
     * 随机创建mac值
     *
     * @return mac
     */
    public static String getRandomMac() {
        String[] mac = {
                String.format("%02x", random.nextInt(0xff)),
                String.format("%02x", random.nextInt(0xff)),
                String.format("%02x", random.nextInt(0xff)),
                String.format("%02x", random.nextInt(0xff)),
                String.format("%02x", random.nextInt(0xff)),
                String.format("%02x", random.nextInt(0xff)),
                String.format("%02x", random.nextInt(0xff)),
        };
        return String.join("", mac).toUpperCase();
    }
}
