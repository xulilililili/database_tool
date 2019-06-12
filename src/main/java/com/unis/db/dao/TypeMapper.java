package com.unis.db.dao;

import java.util.List;

/**
 * @author xuli
 * @date 2019/4/17
 */
public interface TypeMapper {

    /**
     * 查询总数
     * @param sql sql
     * @return 返回表总数
     */
    Integer searchTotal(String sql);

    /**
     * 查询总数
     * @param sql sql
     * @param printLog 是否打印日志
     * @return 返回表总数
     */
    Integer searchTotal(String sql, boolean printLog);

    /**
     * 执行删表、建表、建索引和执行存储过程等
     * @param sql sql
     * @return 返回执行时间
     */
    double execute(String sql);

    /**
     * 执行删表、建表、建索引和执行存储过程等
     * @param sql sql
     * @param printLog 是否打印日志
     * @return 返回执行时间
     */
    double execute(String sql, boolean printLog);

    /**
     * 随机取recordId
     * @param sql sql
     * @return recordId列表
     */
    List<Long> getRecordId(String sql);

    /**
     * 取到tableName 列表
     * @param sql sql
     * @return tableName 列表
     */
    List<String> getTableName(String sql);
}
