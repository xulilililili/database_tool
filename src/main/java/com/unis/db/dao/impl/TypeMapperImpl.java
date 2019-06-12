package com.unis.db.dao.impl;

import com.unis.db.dao.TypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author xuli
 * @date 2019/4/17
 */
@Repository
public class TypeMapperImpl implements TypeMapper {

    private static final Logger logger = LoggerFactory.getLogger(TypeMapperImpl.class);

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Override
    public Integer searchTotal(String sql) {
        return jdbcTemplate.queryForObject(sql, Integer.class);
    }

    @Override
    public Integer searchTotal(String sql, boolean printLog) {
        Long startTime = System.currentTimeMillis();
        int total = searchTotal(sql);
        Long endTime = System.currentTimeMillis();
        if (printLog) {
            logger.info("[ SELECT COUNT ]:[{}] finished with {}s", sql, (endTime - startTime) / 1000.0);
        }
        return total;
    }

    @Override
    public double execute(String sql) {
        Long startTime = System.currentTimeMillis();
        jdbcTemplate.execute(sql);
        Long endTime = System.currentTimeMillis();
        return (endTime - startTime) / 1000.0;
    }

    @Override
    public double execute(String sql, boolean printLog) {
        double cost = execute(sql);
        if (printLog) {
            logger.info("[ EXECUTE SQL ]:[{}] finished with {}s", sql, cost);
        }
        return cost;
    }

    @Override
    public List<Long> getRecordId(String sql) {
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getLong("recordid"));
    }

    @Override
    public List<String> getTableName(String sql) {
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("tablename"));
    }
}
