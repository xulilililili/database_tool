package com.unis.db.service;

/**
 * @author xuli
 * @date 2019/4/17
 */
public interface VehicleService {
    /**
     * 生成车辆数据
     *
     * @param passTime       时间戳
     * @param recordID       主键
     * @param partitionState 是否分区
     * @return 数据
     */
    String makeVehicleData(long passTime, long recordID, boolean partitionState);

    /**
     * 随机生成结构化检索sql
     * @param prefixSql 前缀
     * @param suffixSql 后缀
     * @param number 编号
     * @return 随机生成的sql
     */
    String spliceRandomSql(String prefixSql, String suffixSql, int number);
    /**
     * 结构化查询
     * @param tableName 表名
     * @param startDate 起始日期
     * @param endDate 结束日期
     * @param number 编号
     * @return 数据量
     */
    double searchVehicleStructured(String tableName, String startDate, String endDate, int number);

}
