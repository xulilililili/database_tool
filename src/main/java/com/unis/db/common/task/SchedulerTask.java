package com.unis.db.common.task;

import com.unis.db.common.utils.TableUtils;
import com.unis.db.common.utils.ToolUtils;
import com.unis.db.dao.VehicleMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Random;

/**
 * @author xuli
 * @date 2019/4/1
 */
@Component
public class SchedulerTask {

    @Value("${generate.algorithm-version}")
    private String version;

    @Value("${generate.remain-date}")
    private int remainDate;

    public String type = "vehicle";

    @Autowired
    VehicleMapper vehicleMapper;

    private Random random = new Random();

    /**
     * 得到随机查询的表名
     *
     * @return 表名
     */
    public String getTableName(int number) {
        String tableNameSql = String.format(
                "select tablename from pg_tables where tablename like '%sstructured_%s%%' order by tablename", type, version);
        //return String.format("viid_%s.%s", type, TableUtils.getRandom(tableNameSql, number));
        return "";
    }

    /**
     * 定时器2分钟查询一次，表是正在插入数据的表(根据留存期排序过后是第1张表)，recordid是随机选取
     */
    //@Scheduled(cron = "0 0/2 * * * ? ")
    public void queryInsertingTableTask() {
        String field = "recordid";
        String tableName = getTableName(remainDate + 1);
        String recordIDSql = String.format("select %s from %s offset 500 limit 200", field, tableName);
        //String recordID = TableUtils.getRandom(recordIDSql, random.nextInt(100));
        //TableUtils.query(tableName, field, recordID, true);
    }

    /**
     * 定时器每分钟查询一次，表在150张表中随机挑选表，recordid是随机选取
     */
    //@Scheduled(cron = "0,30 * * * * ? ")
    public void queryRecordIDTask() {
        String field = "recordid";
        //随机值的取法需要过滤第一个表和最后一个表
        String tableName = getTableName(random.nextInt(remainDate - 1) + 1);
        //使用offset 会让取到的recordID是随机的
        String recordIDSql = String.format("select %s from %s offset 500 limit 200", field, tableName);
        //String recordID = TableUtils.getRandom(recordIDSql, random.nextInt(100));
       // TableUtils.query(tableName, field, recordID);
    }

    /**
     * 定时器每每分钟查询一次，表在150张表中随机挑选，车牌是随机生成
     */
    //@Scheduled(cron = "15,45 * * * * ? ")
    public void queryPlateNoTask() {
        String field = "plateno";
        //随机值的取法需要过滤第一个表和最后一个表
        String tableName = getTableName(random.nextInt(remainDate - 1) + 1);
        //TableUtils.query(tableName, field, ToolUtils.createPlateNo());
    }
}
