package com.unis.db.service.impl;

import com.unis.db.common.enums.TableTypeEnum;
import com.unis.db.common.utils.ThreadUtils;
import com.unis.db.controller.dto.QueryTaskByConditions;
import com.unis.db.dao.TypeMapper;
import com.unis.db.service.TypeService;
import com.unis.db.service.VehicleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * @author xuli
 * @date 2019/6/10
 */
@Service
public class TypeServiceImpl implements TypeService {
    private static final Logger logger = LoggerFactory.getLogger(TypeServiceImpl.class);

    @Autowired
    TypeMapper typeMapper;
    @Autowired
    VehicleService vehicleService;
    @Autowired
    private ThreadUtils threadUtils;

    private Random random = new Random();

    @Override
    public void dropTable(String tableName) {
        String dropSql = String.format("drop table if exists %s", tableName);
        typeMapper.execute(dropSql);
        logger.info("[ DROP TABLE ]:[{}] ", dropSql);
    }

    @Override
    public void createTableLike(String tableName, String baseTableName, boolean index) {
        String dropSql = String.format("drop table if exists %s", tableName);
        typeMapper.execute(dropSql);
        String createSql = String.format("create table %s (like %s) ", tableName, baseTableName);
        if (index) {
            createSql = String.format("create table %s (like %s including indexes) ", tableName, baseTableName);
        }
        typeMapper.execute(createSql);
        logger.info("[ CREATE TABLE ]: [{}] ", createSql);
    }

    @Override
    public void createPgIndex(String tableName, String type) {
        String[] array = tableName.split("\\.");
        Long begin = System.currentTimeMillis();
        Map<String, String> map = TableTypeEnum.getIndexMessageByType(type);
        for (String key : map.keySet()) {
            String createIndexSql = String.format("create index %s_%s on %s %s", array[1], key, tableName, map.get(key));
            typeMapper.execute(createIndexSql);
        }
        String analyzeSql = String.format("analyze %s", tableName);
        typeMapper.execute(analyzeSql);
        double cost = (System.currentTimeMillis() - begin) / 1000.0;
        logger.info("[ CREATE INDEX ]: [{}] finished with {}s", tableName, cost);
    }

    @Override
    public void executeCreateProc(String procName, String version, String date) {
        String procSql = String.format("select %s('%s','%s')", procName, version, date);
        double cost = typeMapper.execute(procSql);
        logger.info("[ EXECUTE PROCEDURE ]:[{}] finished with {}s", procSql, cost);
    }

    @Override
    public int searchTotal(String tableName) {
        String querySql = String.format("select count(*) from  %s", tableName);
        return typeMapper.searchTotal(querySql);
    }

    @Override
    public double getRecordIdPartition(String tableName, String startDate, String endDate) {
        Random random = new Random();
        int randomNum = random.nextInt(50000);
        String sql = String.format("select recordid from %s where cur_date between '%s' and '%s' order by recordid offset %s limit 50", tableName, startDate, endDate, randomNum);
        List<Long> recordIdList = typeMapper.getRecordId(sql);
        StringBuilder sb = new StringBuilder();
        for (long recordId : recordIdList) {
            sb.append(recordId).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        String querySql = String.format("select * from %s where cur_date between '%s' and '%s' and recordid in ( %s )"
                , tableName, startDate, endDate, sb.toString());
        double cost = typeMapper.execute(querySql);
        logger.info("[ SELECT RECORDID ]:[{}] finished with {}s", querySql, cost);
        return cost;
    }

    @Override
    public boolean queryTask(QueryTaskByConditions queryTaskByConditions) {
        String type = queryTaskByConditions.getType();
        String algorithm = queryTaskByConditions.getAlgorithm();
        int threadNum = queryTaskByConditions.getThreadNum();
        int timeInterval = queryTaskByConditions.getTimeInterval();
        String baseTableName = TableTypeEnum.getTableNameByType(type);
        for (int i = 0; i < threadNum; i++) {
            QueryThread task = new QueryThread(algorithm, timeInterval, baseTableName);
            threadUtils.submit(task, false);
        }
        if (threadUtils.waitTask(threadNum, false)) {
            return true;
        }
        return false;
    }

    @Override
    public String getRandomTable(String schemaName, String tableLike) {
        String tableName = "";
        int num = 0;
        String totalSql = String.format("select count(*) from pg_tables where schemaname='%s' and tablename like '%s%%'",
                schemaName, tableLike);
        int total = typeMapper.searchTotal(totalSql);
        //不能取到正在插入数据的表，固total-1
        int randomNum = random.nextInt(total - 1);
        String sql = String.format("select tablename from pg_tables where schemaname='%s' and tablename like '%s%%' order by tablename",
                schemaName, tableLike);
        List<String> tableNameList = typeMapper.getTableName(sql);
        for (String name : tableNameList) {
            tableName = name;
            if (num++ == randomNum) {
                return tableName;
            }
        }
        return tableName;
    }

    /**
     * 内部类，线程
     */
    private class QueryThread implements Callable {
        private final Logger logger = LoggerFactory.getLogger(QueryThread.class);

        /**
         * 算法版本
         */
        private String algorithm;
        /**
         * 时间间隔
         */
        private int timeInterval;
        /**
         * 模板表
         */
        private String baseTableName;

        private QueryThread(String algorithm, int timeInterval, String baseTableName) {
            this.algorithm = algorithm;
            this.timeInterval = timeInterval;
            this.baseTableName = baseTableName;
        }

        @Override
        public Boolean call() {
            String schemaName = baseTableName.split("\\.")[0];
            String tableLike = String.format("%s_%s", baseTableName, algorithm).split("\\.")[1];
            String tableName;
            String prefixSql;
            String suffixSql = " order by passtime,recordid limit 10 offset 0 ";
            String fullSql;
            while (true) {
                try {
                    //延时
                    Thread.sleep((random.nextInt(timeInterval) + timeInterval / 2) * 1000);
                    //随机得到存在的表名
                    tableName = String.format("%s.%s", schemaName, getRandomTable(schemaName, tableLike));
                    prefixSql = String.format("select * from %s where 1=1 ", tableName);
                    //车辆数据：拼接随机条件的sql
                    fullSql = vehicleService.spliceRandomSql(prefixSql, suffixSql, random.nextInt(10) + 1);
                    typeMapper.execute(fullSql, true);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

}
