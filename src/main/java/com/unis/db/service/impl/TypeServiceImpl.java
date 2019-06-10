package com.unis.db.service.impl;

import com.unis.db.common.enums.TableTypeEnum;
import com.unis.db.common.enums.VehicleEnum;
import com.unis.db.common.utils.RandomUtils;
import com.unis.db.dao.TypeMapper;
import com.unis.db.service.TypeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author xuli
 * @date 2019/6/10
 */
@Service
public class TypeServiceImpl implements TypeService {
    private static final Logger logger = LoggerFactory.getLogger(TypeServiceImpl.class);

    @Autowired
    TypeMapper typeMapper;

    @Override
    public void dropTable(String tableName) {
        String dropSql = String.format("drop table if exists %s", tableName);
        typeMapper.execute(dropSql);
        logger.info("[ DROP TABLE ]:{} ", dropSql);
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
        logger.info("[ CREATE TABLE ]: {} ", createSql);
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
        logger.info("[ CREATE INDEX ]: {} finished with {}s", tableName, cost);
    }

    @Override
    public void executeCreateProc(String procName, String version, String date) {
        String procSql = String.format("select %s('%s','%s')", procName, version, date);
        double cost = typeMapper.execute(procSql);
        logger.info("[ EXECUTE PROCEDURE ]:{} finished with {}s", procSql, cost);
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
        String sql = String.format("select recordid from %s where cur_date between '%s' and '%s' offset %s limit 50", tableName, startDate, endDate, randomNum);
        List<Long> recordIdList = typeMapper.getRecordId(sql);
        StringBuilder sb = new StringBuilder();
        for (long recordId : recordIdList) {
            sb.append(recordId).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        String querySql = String.format("select * from %s where cur_date between '%s' and '%s' and recordid in ( %s )"
                , tableName, startDate, endDate, sb.toString());
        double cost = typeMapper.execute(querySql);
        logger.info("[ SELECT RECORDID ]:{} finished with {}s", querySql, cost);
        return cost;
    }

}
