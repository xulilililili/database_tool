package com.unis.db.service.impl;

import com.unis.db.common.utils.ThreadUtils;
import com.unis.db.common.utils.ToolUtils;
import com.unis.db.common.enums.TableTypeEnum;
import com.unis.db.common.enums.DatabaseTypeEnum;
import com.unis.db.controller.dto.UseByConditions;
import com.unis.db.service.MakeDataService;
import com.unis.db.service.VehicleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


/**
 * @author xuli
 * @date 2019/2/12
 */
@Service
public class MakeDataServiceImpl implements MakeDataService {
    private static final Logger logger = LoggerFactory.getLogger(MakeDataServiceImpl.class);

    /**
     * 线程数
     */
    @Value("${generate.thread-number}")
    private int threadNum;
    /**
     * 天数
     */
    @Value("${generate.days}")
    private int days;

    /**
     * 起始日期
     */
    @Value("${generate.start-date}")
    private String startDate;

    /**
     * 循环次数
     */
    @Value("${generate.loop}")
    private int loop;

    /**
     * copyIn 单次提交条数
     */
    @Value("${generate.batch-size}")
    private int batchSize;

    /**
     * 留存期
     */
    @Value("${generate.remain-date}")
    private int remainDate;

    /**
     * 数据库类型: gp ,pg
     */
    @Value("${generate.database-type}")
    private String databaseType;

    /**
     * 建表是否有索引:true false
     */
    @Value("${generate.index}")
    private boolean index;

    /**
     * 算法版本
     */
    @Value("${generate.algorithm-version}")
    private String version;

    @Autowired
    private ThreadUtils threadUtils;

    @Autowired
    VehicleService vehicleService;

    @Override
    public Boolean useThread(String tableName, boolean partitionState) {
        Long begin = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            CopyInDataService task = new CopyInDataService(tableName, loop, batchSize, partitionState);
            threadUtils.submit(task);
        }
        if (threadUtils.waitTask(threadNum)) {
            double cost = (System.currentTimeMillis() - begin) / 1000.0;
            int count = vehicleService.searchTotal(tableName);
            logger.info("[ INSERT DATA ] :{} finished with {}s and insert {} pieces of data ", tableName, cost, count);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Boolean makeDataByPartition(UseByConditions useByConditions) {
        String type=useByConditions.getType();
        String procName = String.format("viid_%s.%sstructured_create_partition_proc", type, type);
        String month = startDate.substring(0, 6);
        vehicleService.executeCreateProc(procName, version, month);
        String baseTableName = TableTypeEnum.getTableNameByType(type);
        String partitionTableName = String.format("%s_%s_%s", baseTableName, version, month);
        if (useThread(partitionTableName, true)) {
            int maxDay = ToolUtils.getMaxDay(month);
            String date = month + "01";
            for (int i = 0; i < maxDay; i++) {
                String tableName = String.format("%s_%s_%s", baseTableName, version, date);
                vehicleService.createIndex(tableName);
                date = ToolUtils.getDay(date, 1);
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Boolean makeData(UseByConditions useByConditions) {
        String type=useByConditions.getType();
        String baseTableName = TableTypeEnum.getTableNameByType(type);
        String date = startDate;
        for (int j = 0; j < days; j++) {
            String tableName = String.format("%s_%s_%s", baseTableName, version, date);
            vehicleService.createTableLike(tableName, baseTableName, index);
            if (useThread(tableName, false)) {
                if (DatabaseTypeEnum.GP.getType().equals(databaseType)) {
                    String procName = String.format("viid_%s.%sstructured_create_index_proc", type, type);
                    vehicleService.executeCreateProc(procName, version, date);
                    vehicleService.dropTable(String.format("%s_%s_%s", baseTableName, version, ToolUtils.getDay(date, -remainDate)));
                } else {
                    vehicleService.createIndex(tableName);
                }
            } else {
                return false;
            }
            date = ToolUtils.getDay(date, 1);
        }
        return true;
    }
}

