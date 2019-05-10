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
     * 删除表
     *
     * @param tableName 表名
     */
    void dropTable(String tableName);

    /**
     * 用like 来建表 如果表存在则删除
     *
     * @param tableName     表名
     * @param baseTableName 模板表
     * @param index         是否有索引
     */
    void createTableLike(String tableName, String baseTableName, boolean index);

    /**
     * 创建索引
     *
     * @param tableName 表名
     * @param type 类型
     */
    void createIndex(String tableName,String type);

    /**
     * 执行存储过程
     *
     * @param procName 存储过程名
     * @param version  算法版本
     * @param date     日期
     */
    void executeCreateProc(String procName, String version, String date);

    /**
     * 查询总数 需要优化
     *
     * @param tableName 表名
     * @return 数据量
     */
    int searchTotal(String tableName);

    /**
     * 查询100个recordid所需时间
     *
     * @param tableName 表名
     * @param startDate 起始分区
     * @param endDate   结束分区
     * @return 执行时间
     */
    double getRecordIdPartition(String tableName, String startDate, String endDate);

    /**
     * 结构化查询
     * @param tableName 表名
     * @param startDate 起始日期
     * @param endDate 结束日期
     * @param number 编号
     * @return 数据量
     */
    double searchStructured(String tableName, String startDate, String endDate, int number);

}
