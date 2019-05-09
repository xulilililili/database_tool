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
     * 数据库类型: gp ,pg
     */
    @Value("${generate.database-type}")
    private String databaseType;

    @Autowired
    private ThreadUtils threadUtils;

    @Autowired
    VehicleService vehicleService;

    @Override
    public Boolean useThread(String tableName, boolean partitionState, UseByConditions useByConditions) {
        Long begin = System.currentTimeMillis();
        int loop = useByConditions.getLoop();
        int batchSize = useByConditions.getBatchSize();
        int threadNum = useByConditions.getThreadNum();
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
        String algorithm = useByConditions.getAlgorithm();
        String type = useByConditions.getType();
        String baseTableName = TableTypeEnum.getTableNameByType(type);
        String procName = String.format("%s_create_partition_proc", baseTableName);
        String month = useByConditions.getStartDate().substring(0, 6);
        vehicleService.executeCreateProc(procName, algorithm, month);
        String partitionTableName = String.format("%s_%s_%s", baseTableName, algorithm, month);
        if (useThread(partitionTableName, true, useByConditions)) {
            int maxDay = ToolUtils.getMaxDay(month);
            String date = month + "01";
            for (int i = 0; i < maxDay; i++) {
                String tableName = String.format("%s_%s_%s", baseTableName, algorithm, date);
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
        String algorithm = useByConditions.getAlgorithm();
        String date = useByConditions.getStartDate();
        int days = useByConditions.getDays();
        boolean index = useByConditions.isIndex();
        int remainDate = useByConditions.getRemainDate();
        String type = useByConditions.getType();
        String baseTableName = TableTypeEnum.getTableNameByType(type);
        for (int j = 0; j < days; j++) {
            String tableName = String.format("%s_%s_%s", baseTableName, algorithm, date);
            vehicleService.createTableLike(tableName, baseTableName, index);
            if (useThread(tableName, false, useByConditions)) {
                if (DatabaseTypeEnum.GP.getType().equals(databaseType)) {
                    String procName = String.format("%s_create_index_proc", baseTableName);
                    vehicleService.executeCreateProc(procName, algorithm, date);
                    vehicleService.dropTable(String.format("%s_%s_%s", baseTableName, algorithm, ToolUtils.getDay(date, -remainDate)));
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

