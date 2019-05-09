package com.unis.db.common.enums;


import java.util.HashMap;
import java.util.Map;

/**
 * @author xuli
 * @date 2019/3/26
 */
public enum TableTypeEnum {
    /**
     * 动态人脸
     */
    FaceSnap("facesnap", "recordid_idx.(recordid)|passtime_idx.(passtime)|" +
            "join_idx.(deviceid,ageuplimit,agelowerlimit,gendercode)","viid_facesnap.facesnapstructured"),
    /**
     * 人员
     */
    Person("person", "recordid_idx.(recordid)|snaptimestamp_idx.(snaptimestamp)|" +
            "join_idx.(deviceid,gendercode,ageuplimit,agelowerlimit)","viid_person.personstructured"),
    /**
     * 车辆
     */
    Vehicle("vehicle", "recordid_idx.(recordid)|passtime_idx.(passtime)|" +
            "plateno_like_idx.using gin (plateno gin_trgm_ops)|plateno_idx.(plateno)|motorvehicleid_idx.(motorvehicleid)|" +
            "join_idx.(deviceid,tollgateid,vehiclecolor,platecolor,plateclass,vehicleclass,vehiclebrand,direction)","viid_vehicle.vehiclestructured"),
    /**
     * 人脸归档
     */
    FaceArchive("facearchive","faceid_idx.(faceid)|peopleid_idx.(peopleid)|timestamp_idx.(timestamp)|" +
            "join_idx.(deviceid,tollgateid,type)","viid_facestatic.face_archive"),
    /**
     * Wifi:终端功能
     */
    TerminalFeature("terminalfeature","record_id_idx.(record_id)|mac_idx.(mac)|capture_time_idx.(capture_time)|" +
            "certificate_code_idx.(certificate_code)|collection_equipment_id_idx.(collection_equipment_id)|","viid_iot.terminal_feature");

    private String type;
    private String indexMessage;
    private String tableName;

    TableTypeEnum(String type, String indexMessage,String tableName) {
        this.type = type;
        this.indexMessage = indexMessage;
        this.tableName=tableName;
    }

    public String getType() {
        return type;
    }

    public String getIndexMessage() {
        return indexMessage;
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * 根据type来获取表名
     * @param type 类型
     * @return String
     */
    public static String getTableNameByType(String type){
        for (TableTypeEnum tableTypeEnum : TableTypeEnum.values()) {
            if (type.equals(tableTypeEnum.getType())) {
                return tableTypeEnum.getTableName();
            }
        }
        return null;
    }

    /**
     * 根据type来获取索引信息
     *
     * @param type 类型
     * @return Map
     */
    public static Map<String, String> getIndexMessageByType(String type) {
        Map<String, String> map = new HashMap<>();
        for (TableTypeEnum tableTypeEnum : TableTypeEnum.values()) {
            if (type.equals(tableTypeEnum.getType())) {
                String[] array = tableTypeEnum.getIndexMessage().split("\\|");
                for (String value : array) {
                    map.put(value.split("\\.")[0], value.split("\\.")[1]);
                }
            }
        }
        return map;
    }

}
