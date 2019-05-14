package com.unis.db.service.impl;

import com.unis.db.common.enums.TableTypeEnum;
import com.unis.db.common.enums.VehicleEnum;
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

    private VehicleStructured vehicle = new VehicleStructured();

    private Random random = new Random();

    @Autowired
    VehicleMapper vehicleMapper;

    @Override
    public String makeVehicleData(long passTime, long recordID, boolean partitionState) {
        vehicle.setRecordID(recordID);
        vehicle.setMotorVehicleID(Integer.toString(random.nextInt(100000000)));
        vehicle.setTollgateID(VehicleEnum.TOLLGATE_PREFIX + VehicleEnum.DEVICE_SUFFIX[random.nextInt(VehicleEnum.DEVICE_SUFFIX.length)]);
        vehicle.setDeviceID(VehicleEnum.DEVICE_PREFIX + VehicleEnum.DEVICE_SUFFIX[random.nextInt(VehicleEnum.DEVICE_SUFFIX.length)]);
        vehicle.setStorageUrlCloseShot("/picture/2018-08-17T08-29-20Z/1534495880605_3865.jpg");
        vehicle.setStorageUrlPlate("1");
        vehicle.setStorageUrlDistantShot("1");
        vehicle.setStorageUrlCompound("1");
        vehicle.setStorageUrlBreviary("1");
        vehicle.setLaneNo(random.nextInt(8) + 1);
        vehicle.setHasPlate(random.nextInt(2));
        vehicle.setPlateClass(VehicleEnum.PLATE_CLASS[random.nextInt(VehicleEnum.PLATE_CLASS.length)]);
        vehicle.setPlateColor(VehicleEnum.PLATE_COLOR[random.nextInt(VehicleEnum.PLATE_COLOR.length)]);
        vehicle.setPlateNo(RandomUtils.getRandomPlateNo());
        vehicle.setSpeed(0.0000);
        vehicle.setDirection(random.nextInt(9) + 1);
        vehicle.setDrivingStatusCode("1");
        vehicle.setVehicleLeftTopX(0.0000);
        vehicle.setVehicleLeftTopY(0.0000);
        vehicle.setVehicleRightBtmX(0.0000);
        vehicle.setVehicleRightBtmY(0.0000);
        vehicle.setVehicleClass(VehicleEnum.VEHICLE_CLASS[random.nextInt(VehicleEnum.VEHICLE_CLASS.length)]);
        vehicle.setVehicleBrand(VehicleEnum.VEHICLE_BRAND[random.nextInt(VehicleEnum.VEHICLE_BRAND.length)]);
        vehicle.setVehicleModel("1");
        vehicle.setVehicleStyles("1");
        vehicle.setVehicleColor(VehicleEnum.VEHICLE_COLOR[random.nextInt(VehicleEnum.VEHICLE_COLOR.length)]);
        vehicle.setVehicleColorDepth(random.nextInt(2));
        vehicle.setPassTime(passTime);
        vehicle.setVehicleAppearTime(0L);
        vehicle.setVehicleDisappearTime(0L);
        vehicle.setPlateReliability(0);
        vehicle.setPlateCharReliability(0);
        vehicle.setBrandReliability(0);
        vehicle.setDriverFace("1");
        vehicle.setViceDriverFace("1");
        vehicle.setSunVisor(random.nextInt(2));
        vehicle.setSafetyBelt(random.nextInt(2));
        vehicle.setCalling(random.nextInt(2));
        vehicle.setVehicleAttitude(random.nextInt(3));
        vehicle.setIsPerfumeBottle(random.nextInt(2));
        vehicle.setIsOrnament(random.nextInt(2));
        vehicle.setIsTissue(random.nextInt(2));
        vehicle.setIsInspectionMark(random.nextInt(2));
        vehicle.setIsSpareWheel(random.nextInt(2));
        vehicle.setIsMuckCar(random.nextInt(2));
        vehicle.setIsHazardousTanker(random.nextInt(2));
        vehicle.setIsSunroof(random.nextInt(2));
        vehicle.setVehicleFrontItem("" + VehicleEnum.VEHICLE_FRONT_ITEM[random.nextInt(VehicleEnum.VEHICLE_FRONT_ITEM.length)]);
        vehicle.setDescOfFrontItem("");
        vehicle.setHitMarkInfo(random.nextInt(3));
        vehicle.setRearviewMirror("");
        vehicle.setPlateNoAttach("");
        vehicle.setVehicleWindow("");
        vehicle.setIsLuggageRack(random.nextInt(2));
        vehicle.setCoSunvisor(random.nextInt(2));
        vehicle.setCoSafetyBelt(random.nextInt(2));
        vehicle.setIsYellowLabel(random.nextInt(2));

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
    public void createIndex(String tableName, String type) {
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
                        , sql, RandomUtils.getRandomDeviceIds(10), VehicleEnum.VEHICLE_COLOR[random.nextInt(VehicleEnum.VEHICLE_COLOR.length)],
                        VehicleEnum.VEHICLE_CLASS[random.nextInt(VehicleEnum.VEHICLE_CLASS.length)], sql01);
                break;
            case 6:
                fullSql = String.format("%s and deviceid in (%s) and vehiclecolor = %s and vehicleclass = '%s' %s"
                        , sql, RandomUtils.getRandomDeviceIds(20), VehicleEnum.VEHICLE_COLOR[random.nextInt(VehicleEnum.VEHICLE_COLOR.length)],
                        VehicleEnum.VEHICLE_CLASS[random.nextInt(VehicleEnum.VEHICLE_CLASS.length)], sql01);
                break;
            case 7:
                fullSql = String.format("%s and deviceid in (%s) and vehiclecolor = %s and vehicleclass = '%s' and plateclass = %s %s"
                        , sql, RandomUtils.getRandomDeviceIds(10), VehicleEnum.VEHICLE_COLOR[random.nextInt(VehicleEnum.VEHICLE_COLOR.length)],
                        VehicleEnum.VEHICLE_CLASS[random.nextInt(VehicleEnum.VEHICLE_CLASS.length)], VehicleEnum.PLATE_CLASS[random.nextInt(VehicleEnum.PLATE_CLASS.length)], sql01);
                break;
            case 8:
                fullSql = String.format("%s and deviceid in (%s) and vehiclecolor = %s and vehicleclass = '%s' and plateclass = %s %s"
                        , sql, RandomUtils.getRandomDeviceIds(20), VehicleEnum.VEHICLE_COLOR[random.nextInt(VehicleEnum.VEHICLE_COLOR.length)],
                        VehicleEnum.VEHICLE_CLASS[random.nextInt(VehicleEnum.VEHICLE_CLASS.length)], VehicleEnum.PLATE_CLASS[random.nextInt(VehicleEnum.PLATE_CLASS.length)], sql01);
                break;
            case 9:
                fullSql = String.format("%s and vehiclecolor = %s and vehicleclass = '%s'  %s"
                        , sql, VehicleEnum.VEHICLE_COLOR[random.nextInt(VehicleEnum.VEHICLE_COLOR.length)],
                        VehicleEnum.VEHICLE_CLASS[random.nextInt(VehicleEnum.VEHICLE_CLASS.length)], sql01);
                break;
            case 10:
                fullSql = String.format("%s and vehiclecolor = %s and vehicleclass = '%s' and plateclass = %s  %s"
                        , sql, VehicleEnum.VEHICLE_COLOR[random.nextInt(VehicleEnum.VEHICLE_COLOR.length)],
                        VehicleEnum.VEHICLE_CLASS[random.nextInt(VehicleEnum.VEHICLE_CLASS.length)],
                        VehicleEnum.PLATE_CLASS[random.nextInt(VehicleEnum.PLATE_CLASS.length)], sql01);
                break;
            default:
                fullSql = sql;
                break;
        }
        return vehicleMapper.execute(fullSql);
    }

}
