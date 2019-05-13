package com.unis.db.service.impl;

import com.unis.db.common.enums.TableTypeEnum;
import com.unis.db.common.utils.DateUtils;
import com.unis.db.common.utils.RandomUtils;
import com.unis.db.dao.VehicleMapper;
import com.unis.db.model.VehicleStructured;
import com.unis.db.service.VehicleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author xuli
 * @date 2019/4/17
 */
@Service
public class VehicleServiceImpl implements VehicleService {

    private static final Logger logger = LoggerFactory.getLogger(VehicleServiceImpl.class);

    /**
     * 车辆卡口 共103个，
     * 例子:50010500001191000xxx
     */
    private static final String DEVICE_PREFIX = "50010500001191000";
    private static final String TOLLGATE_PREFIX = "50010500001211000";
    private static final String[] DEVICE_SUFFIX = {"086", "087", "089", "090", "092", "093", "094", "095", "096", "097", "098", "099", "100", "101", "102", "103", "104", "105", "106", "107", "108", "109", "110", "111", "112", "113", "114", "115", "116", "117", "118", "119", "120", "121", "122", "123", "124", "125", "126", "127", "128", "129", "130", "131", "132", "133", "134", "135", "136", "137", "138", "139", "140", "141", "142", "143", "144", "145", "146", "147", "148", "149", "150", "151", "152", "153", "154", "155", "156", "157", "158", "159", "160", "161", "162", "163", "164", "165", "166", "167", "168", "169", "170", "171", "172", "173", "174", "175", "176", "177", "178", "179", "180", "181", "182", "183", "184", "185", "186", "187", "188", "189", "190"};
    /**
     * 车牌类型
     */
    private static final int[] PLATE_CLASS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22, 23, 24, 25, 26, 27, 31, 32, 99};
    /**
     * 车牌颜色
     */
    private static final int[] PLATE_COLOR = {1, 2, 5, 6, 9};
    /**
     * 车辆品牌
     */
    private static final int[] VEHICLE_BRAND = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30
            , 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 65, 66, 67, 68, 69,
            70, 71, 73, 75, 76, 81, 82, 83, 86, 87, 88, 89, 90, 92, 94, 95, 96, 97, 103, 104, 107, 108, 109, 110, 112, 113, 115, 117, 119, 121, 122, 124, 128, 136,
            144, 155, 167, 176, 191, 201, 230, 10001, 10003, 10004, 10006, 10007, 10008, 10009, 10010, 10011, 10012, 10013, 10014, 10015, 10016, 10017, 10018, 10019, 10020, 10021, 10022,
            10023, 10024, 10025, 10026, 10027, 10028, 10029, 10030, 10031, 10033, 10034, 10035, 10036, 10037, 10038, 10039, 10040, 10041, 10042, 10043, 10044, 10045, 10047, 10048, 10049, 10050,
            10051, 10052, 10053, 10054, 10056, 10057, 10059, 10060, 10061, 10062, 10063, 10064, 10065, 10066, 10067, 10068, 10070, 10071, 10072, 10073, 10074, 10075, 10076, 10078};
    /**
     * 车辆颜色
     */
    private static final int[] VEHICLE_COLOR = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 99};
    /**
     * 车辆类型
     */
    private static final String[] VEHICLE_CLASS = {"B11", "B12", "B13", "B14", "B15", "B16", "B17", "B18", "B19", "B1A", "B1B", "B21", "B22", "B23", "B24", "B25", "B26", "B27", "B28", "B29", "B2A", "B2B", "B31", "B32", "B33", "B34", "B35", "B36", "B37", "B38", "B39", "D11", "D12", "G11", "G12", "G13", "G14", "G15", "G16", "G17", "G18", "G19", "G21", "G22", "G23", "G24", "G25", "G26", "G27", "G28", "G29", "G31", "G32", "G33", "G34", "G35", "G36", "G37", "G38", "H11", "H12", "H11", "H12", "H13", "H14", "H15", "H16", "H17", "H18", "H19", "H21", "H22", "H23", "H24", "H25", "H26", "H27", "H28", "H29", "H31", "H32", "H33", "H34", "H35", "H36", "H37", "H38", "H39", "H41", "H42", "H43", "H44", "H45", "H46", "H47", "H51", "H52", "H53", "H54", "H55", "J11", "J12", "J13", "K11", "K12", "K13", "K14", "K15", "K16", "K17", "K18", "K21", "K22", "K23", "K24", "K25", "K26", "K27", "K28", "K31", "K32", "K33", "K34", "K38", "K41", "K42", "K43", "M11", "M12", "M13", "M14", "M15", "Q11", "Q12", "Q21", "Q22", "Q31", "Q32", "T11", "T21", "T22", "T23", "X99", "Z11", "Z21", "Z31", "Z41", "Z51", "Z71"};

    private VehicleStructured vehicle = new VehicleStructured();

    private Random random = new Random();

    @Autowired
    VehicleMapper vehicleMapper;

    @Override
    public String makeVehicleData(long passTime, long recordID, boolean partitionState) {
        vehicle.setRecordID(recordID);
        vehicle.setMotorVehicleID(Integer.toString(random.nextInt(100000000)));
        vehicle.setTollgateID(TOLLGATE_PREFIX + DEVICE_SUFFIX[random.nextInt(DEVICE_SUFFIX.length)]);
        vehicle.setDeviceID(DEVICE_PREFIX + DEVICE_SUFFIX[random.nextInt(DEVICE_SUFFIX.length)]);
        vehicle.setStorageUrlCloseShot("/picture/2018-08-17T08-29-20Z/1534495880605_3865.jpg");
        vehicle.setStorageUrlPlate("");
        vehicle.setStorageUrlDistantShot("");
        vehicle.setStorageUrlCompound("");
        vehicle.setStorageUrlBreviary("");
        vehicle.setLaneNo(random.nextInt(8) + 1);
        vehicle.setHasPlate(random.nextInt(2));
        vehicle.setPlateClass(PLATE_CLASS[random.nextInt(PLATE_CLASS.length)]);
        vehicle.setPlateColor(PLATE_COLOR[random.nextInt(PLATE_COLOR.length)]);
        vehicle.setPlateNo(RandomUtils.getRandomPlateNo());
        vehicle.setSpeed(0.0000);
        vehicle.setDirection(random.nextInt(9) + 1);
        vehicle.setDrivingStatusCode("");
        vehicle.setVehicleLeftTopX(0.0000);
        vehicle.setVehicleLeftTopY(0.0000);
        vehicle.setVehicleRightBtmX(0.0000);
        vehicle.setVehicleRightBtmY(0.0000);
        vehicle.setVehicleClass(VEHICLE_CLASS[random.nextInt(VEHICLE_CLASS.length)]);
        vehicle.setVehicleBrand(VEHICLE_BRAND[random.nextInt(VEHICLE_BRAND.length)]);
        vehicle.setVehicleModel("");
        vehicle.setVehicleStyles("");
        vehicle.setVehicleColor(VEHICLE_COLOR[random.nextInt(VEHICLE_COLOR.length)]);
        vehicle.setVehicleColorDepth(random.nextInt(2));
        vehicle.setPassTime(passTime);
        vehicle.setVehicleAppearTime(0L);
        vehicle.setVehicleDisappearTime(0L);
        vehicle.setPlateReliability(0);
        vehicle.setPlateCharReliability(0);
        vehicle.setBrandReliability(0);
        vehicle.setDriverFace("");
        vehicle.setViceDriverFace("");
        vehicle.setSunVisor(random.nextInt(2));
        vehicle.setSafetyBelt(random.nextInt(2));
        vehicle.setCalling(random.nextInt(2));

        String data = vehicle.toString();
        if (partitionState) {
            data = data + "," + DateUtils.longToString(vehicle.getPassTime(), "yyyy-MM-dd");
        }
        return data;
    }

    @Override
    public void dropTable(String tableName) {
        String dropSql = String.format("drop table if exists %s", tableName);
        vehicleMapper.execute(dropSql);
        logger.info("[ DROP TABLE ]:{} ", dropSql);
    }

    @Override
    public void createTableLike(String tableName, String baseTableName, boolean index) {
        String dropSql = String.format("drop table if exists %s", tableName);
        vehicleMapper.execute(dropSql);
        String createSql = String.format("create table %s (like %s) ", tableName, baseTableName);
        if (index) {
            createSql = String.format("create table %s (like %s including indexes) ", tableName, baseTableName);
        }
        vehicleMapper.execute(createSql);
        logger.info("[ CREATE TABLE ]: {} ", createSql);
    }

    @Override
    public void createIndex(String tableName,String type) {
        String[] array = tableName.split("\\.");
        Long begin = System.currentTimeMillis();
        Map<String, String> map = TableTypeEnum.getIndexMessageByType(type);
        for (String key : map.keySet()) {
            String createIndexSql = String.format("create index %s_%s on %s %s", array[1], key, tableName, map.get(key));
            vehicleMapper.execute(createIndexSql);
        }
        String analyzeSql = String.format("analyze %s", tableName);
        vehicleMapper.execute(analyzeSql);
        double cost = (System.currentTimeMillis() - begin) / 1000.0;
        logger.info("[ CREATE INDEX ]: {} finished with {}s", tableName, cost);
    }

    @Override
    public void executeCreateProc(String procName, String version, String date) {
        String procSql = String.format("select %s('%s','%s')", procName, version, date);
        double cost = vehicleMapper.execute(procSql);
        logger.info("[ EXECUTE PROCEDURE ]:{} finished with {}s", procSql, cost);
    }

    @Override
    public int searchTotal(String tableName) {
        String querySql = String.format("select count(*) from  %s", tableName);
        return vehicleMapper.searchTotal(querySql);
    }

    @Override
    public double getRecordIdPartition(String tableName, String startDate, String endDate) {
        String sql = String.format("select recordid from %s where cur_date between '%s' and '%s' offset 1000 limit 100", tableName, startDate, endDate);
        List<Long> recordIdList = vehicleMapper.getRecordId(sql);
        StringBuilder sb = new StringBuilder();
        for (long recordId : recordIdList) {
            sb.append(recordId).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        String querySql = String.format("select * from %s where cur_date between '%s' and '%s' and recordid in ( %s )"
                , tableName, startDate, endDate, sb.toString());
        double cost = vehicleMapper.execute(querySql);
        logger.info("[ SELECT RECORDID ]:{} finished with {}s", querySql, cost);
        return cost;
    }

    @Override
    public double searchStructured(String tableName, String startDate, String endDate, int number) {
        String sql = String.format("select * from %s where cur_date between '%s' and '%s' ", tableName, startDate, endDate);
        String sql01 = " order by passtime,recordid limit 100 offset 0 ";
        Random random = new Random();
        String fullSql;
        switch (number) {
            case 0:
                fullSql = sql;
                break;
            case 1:
                fullSql = String.format("%s and plateno = '%s' %s", sql, RandomUtils.getRandomPlateNo(), sql01);
                break;
            case 2:
                fullSql = String.format("%s and plateno like '%s%%' %s", sql, RandomUtils.getRandomPlateNo().substring(0, 4), sql01);
                break;
            case 3:
                fullSql = String.format("%s and plateno like '%%%s' %s", sql, RandomUtils.getRandomPlateNo().substring(3, 7), sql01);
                break;
            case 4:
                fullSql = String.format("%s and plateno like '%%%s%%' %s", sql, RandomUtils.getRandomPlateNo().substring(2, 6), sql01);
                break;
            case 5:
                fullSql = String.format("%s and deviceid in (%s) and vehiclecolor = %s and vehicleclass = '%s' %s"
                        , sql, RandomUtils.getRandomDeviceIds(10), VEHICLE_COLOR[random.nextInt(VEHICLE_COLOR.length)], VEHICLE_CLASS[random.nextInt(VEHICLE_CLASS.length)], sql01);
                break;
            case 6:
                fullSql = String.format("%s and deviceid in (%s) and vehiclecolor = %s and vehicleclass = '%s' %s"
                        , sql, RandomUtils.getRandomDeviceIds(20), VEHICLE_COLOR[random.nextInt(VEHICLE_COLOR.length)], VEHICLE_CLASS[random.nextInt(VEHICLE_CLASS.length)], sql01);
                break;
            case 7:
                fullSql = String.format("%s and deviceid in (%s) and vehiclecolor = %s and vehicleclass = '%s' and plateclass = %s %s"
                        , sql, RandomUtils.getRandomDeviceIds(10), VEHICLE_COLOR[random.nextInt(VEHICLE_COLOR.length)], VEHICLE_CLASS[random.nextInt(VEHICLE_CLASS.length)], PLATE_CLASS[random.nextInt(PLATE_CLASS.length)], sql01);
                break;
            case 8:
                fullSql = String.format("%s and deviceid in (%s) and vehiclecolor = %s and vehicleclass = '%s' and plateclass = %s %s"
                        , sql, RandomUtils.getRandomDeviceIds(20), VEHICLE_COLOR[random.nextInt(VEHICLE_COLOR.length)], VEHICLE_CLASS[random.nextInt(VEHICLE_CLASS.length)], PLATE_CLASS[random.nextInt(PLATE_CLASS.length)], sql01);
                break;
            case 9:
                fullSql = String.format("%s and vehiclecolor = %s and vehicleclass = '%s'  %s"
                        , sql, VEHICLE_COLOR[random.nextInt(VEHICLE_COLOR.length)], VEHICLE_CLASS[random.nextInt(VEHICLE_CLASS.length)], sql01);
                break;
            case 10:
                fullSql = String.format("%s and vehiclecolor = %s and vehicleclass = '%s' and plateclass = %s  %s"
                        , sql, VEHICLE_COLOR[random.nextInt(VEHICLE_COLOR.length)], VEHICLE_CLASS[random.nextInt(VEHICLE_CLASS.length)], PLATE_CLASS[random.nextInt(PLATE_CLASS.length)], sql01);
                break;
            default:
                fullSql = sql;
                break;
        }
        // return vehicleMapper.searchTotal(fullSql);
        return vehicleMapper.execute(fullSql);
    }

}
