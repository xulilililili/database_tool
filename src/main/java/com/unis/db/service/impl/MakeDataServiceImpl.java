package com.unis.db.service.impl;

import com.unis.db.common.utils.CopyInUtils;
import com.unis.db.common.utils.DateUtils;
import com.unis.db.common.utils.RandomUtils;
import com.unis.db.common.utils.ThreadUtils;
import com.unis.db.common.enums.TableTypeEnum;
import com.unis.db.common.enums.DatabaseTypeEnum;
import com.unis.db.controller.dto.UseByConditions;
import com.unis.db.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;


/**
 * @author xuli
 * @date 2019/2/12
 */
@Service
public class MakeDataServiceImpl implements MakeDataService {
    private static final Logger logger = LoggerFactory.getLogger(MakeDataServiceImpl.class);

    /**
     * 数据库类型: gp和pg
     */
    @Value("${generate.database-type}")
    private String databaseType;

    @Autowired
    private ThreadUtils threadUtils;

    @Autowired
    TypeService typeService;
    @Autowired
    VehicleService vehicleService;
    @Autowired
    FaceSnapService faceSnapService;
    @Autowired
    PersonService personService;
    @Autowired
    TerminalFeatureService terminalFeatureService;

    @Override
    public Boolean useThread(String tableName, boolean partitionState, String date, UseByConditions useByConditions) {
        Long begin = System.currentTimeMillis();
        String type = useByConditions.getType();
        int loop = useByConditions.getLoop();
        int batchSize = useByConditions.getBatchSize();
        int threadNum = useByConditions.getThreadNum();
        for (int i = 0; i < threadNum; i++) {
            LoadDataThread task = new LoadDataThread(tableName, date, type, loop, batchSize, partitionState);
            threadUtils.submit(task);
        }
        if (threadUtils.waitTask(threadNum,true)) {
            double cost = (System.currentTimeMillis() - begin) / 1000.0;
            int count = typeService.searchTotal(tableName);
            logger.info("[ INSERT DATA ] :{} finished with {}s and insert {} pieces of data ", tableName, cost, count);
            return true;
        }
        return false;
    }

    /**
     * 先插入完数据 再创建索引
     * @param useByConditions 类
     * @return
     */
    @Override
    public Boolean makeDataByPartition(UseByConditions useByConditions) {
        String algorithm = useByConditions.getAlgorithm();
        String type = useByConditions.getType();
        boolean index = useByConditions.isIndex();
        String baseTableName = TableTypeEnum.getTableNameByType(type);
        String procName = String.format("%s_create_partition_proc", baseTableName);
        String month = useByConditions.getStartDate().substring(0, 6);
        typeService.executeCreateProc(procName, algorithm, month);
        String partitionTableName = String.format("%s_%s_%s", baseTableName, algorithm, month);
        if (type.equals(TableTypeEnum.TerminalFeature.getType())) {
            partitionTableName = String.format("%s_%s", baseTableName, month);
        }
        if (useThread(partitionTableName, true, month, useByConditions)) {
            int maxDay = DateUtils.getMaxDate(month);
            String date = month + "01";
            for (int i = 0; i < maxDay; i++) {
                String tableName = String.format("%s_%s_%s", baseTableName, algorithm, date);
                if (type.equals(TableTypeEnum.TerminalFeature.getType())) {
                    tableName = String.format("%s_%s", baseTableName, date);
                }
                if (!index) {
                    //typeService.createPgIndex(tableName, type);
                }
                date = DateUtils.getDateByAdd(date, 1);
            }
            return true;
        }
        return false;
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
        //按天建表，创建完之后创建索引
        for (int j = 0; j < days; j++) {
            String tableName = String.format("%s_%s_%s", baseTableName, algorithm, date);
            if (type.equals(TableTypeEnum.TerminalFeature.getType())) {
                tableName = String.format("%s_%s", baseTableName, date);
            }
            typeService.createTableLike(tableName, baseTableName, index);
            if (useThread(tableName, false, date, useByConditions)) {
                //判断gp和pg
                if (DatabaseTypeEnum.GP.getType().equals(databaseType)) {
                    String procName = String.format("%s_create_index_proc", baseTableName);
                    //创建gp索引
                    typeService.executeCreateProc(procName, algorithm, date);
                    typeService.dropTable(String.format("%s_%s_%s", baseTableName, algorithm, DateUtils.getDateByAdd(date, -remainDate)));
                } else {
                    if (!index) {
                        typeService.createPgIndex(tableName, type);
                    }
                }
            } else {
                return false;
            }
            //日期+1天
            date = DateUtils.getDateByAdd(date, 1);
        }
        return true;
    }


    private class LoadDataThread implements Callable {

        private String tableName;

        private String date;

        private String type;

        private int loop;

        private int batchSize;

        private boolean partitionState;

        private LoadDataThread(String tableName, String date, String type, int loop, int batchSize, boolean partitionState) {
            this.tableName = tableName;
            this.date = date;
            this.type = type;
            this.loop = loop;
            this.batchSize = batchSize;
            this.partitionState = partitionState;
        }

        @Override
        public Boolean call() {
            List<String> dataList = new ArrayList<>(batchSize);
            long passTime;
            for (int i = 0; i < loop; i++) {
                for (int j = 0; j < batchSize; j++) {
                    if (partitionState) {
                        passTime = RandomUtils.getRandomPassTimeInMonth(date);
                    } else {
                        passTime = RandomUtils.getRandomPassTime(date, DateUtils.getDateByAdd(date, 1));
                    }
                    long recordID = RandomUtils.getRandomRecordID(passTime);
                    switch (type) {
                        case "vehicle":
                            dataList.add(vehicleService.makeVehicleData(passTime, recordID, partitionState));
                            break;
                        case "facesnap":
                            dataList.add(faceSnapService.makeFaceSnapData(passTime, recordID, partitionState));
                            break;
                        case "person":
                            dataList.add(personService.makePersonData(passTime, recordID, partitionState));
                            break;
                        case "terminal_feature":
                            dataList.add(terminalFeatureService.makeTerminalFeatureData(passTime, recordID, partitionState));
                            break;
                        default:
                            break;
                    }
                }
                String data = String.join("\n", dataList);
                //执行copyIn
                if (!CopyInUtils.copyIn(data, tableName)) {
                    return false;
                }
                dataList.clear();
            }
            return true;
        }
    }
}

