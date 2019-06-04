package com.unis.db.dao.impl;

import com.unis.db.dao.VehicleMapper;
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
public class VehicleMapperImpl implements VehicleMapper {

    private static final Logger logger = LoggerFactory.getLogger(VehicleMapperImpl.class);

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Override
    public Integer searchTotal(String sql) {
        Long startTime = System.currentTimeMillis();
        Integer total = jdbcTemplate.queryForObject(sql, Integer.class);
        Long endTime = System.currentTimeMillis();
        logger.info("[ SELECT COUNT ]:{} finished with {}s", sql, (endTime - startTime) / 1000.0);
        return total;
    }

    @Override
    public double execute(String sql) {
        Long startTime = System.currentTimeMillis();
        jdbcTemplate.execute(sql);
        Long endTime = System.currentTimeMillis();
        //logger.info("[ SELECT * ]:{} finished with {}s", sql, (endTime - startTime) / 1000.0);
        return (endTime - startTime) / 1000.0;
    }

    @Override
    public List<Long> getRecordId(String sql){
        return jdbcTemplate.query(sql,(rs,rowNum) -> rs.getLong("recordid"));
    }

}
