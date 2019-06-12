package com.unis.db.dao;

import com.unis.db.service.TypeService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.*;

/**
 * @author xuli
 * @date 2019/6/10
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class TypeMapperTest {
    @Autowired
    TypeService typeService;
    @Test
    public void test(){

//        Integer integer = typeMapper.searchTotal(" select count(*) from pg_tables where schemaname='viid_vehicle' and tablename like 'vehiclestructured_a050200%'");System.out.println(integer);
    }

    @Test
    public void test1() {
        String randomTable = typeService.getRandomTable("viid_vehicle","vehiclestructured_a050200");
        System.out.println(randomTable);
    }
}