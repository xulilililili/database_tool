package com.unis.db.service.impl;

import com.unis.db.common.utils.CopyInUtils;
import com.unis.db.common.utils.ToolUtils;
import com.unis.db.service.FaceSnapService;
import com.unis.db.service.PersonService;
import com.unis.db.service.VehicleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * @author xuli
 * @date 2019/3/18
 */

public class CopyInDataService implements Callable {

    private static final Logger logger = LoggerFactory.getLogger(CopyInDataService.class);

    private VehicleService vehicleService = new VehicleServiceImpl();

    private FaceSnapService faceSnapService = new FaceSnapServiceImpl();

    private PersonService personService = new PersonServiceImpl();

    private String tableName;

    private int loop;

    private int batchSize;

    private boolean partitionState;

    public CopyInDataService(String tableName, int loop, int batchSize, boolean partitionState) {
        this.tableName = tableName;
        this.loop = loop;
        this.batchSize = batchSize;
        this.partitionState = partitionState;
    }

    @Override
    public Boolean call() {
        List<String> dataList = new ArrayList<>(batchSize);
        String date = tableName.split("_")[3];
        String type = tableName.split("\\.")[0].split("_")[1];
        long passTime;
        for (int i = 0; i < loop; i++) {
            for (int j = 0; j < batchSize; j++) {
                if (partitionState) {
                    passTime = ToolUtils.createPassTime(date);
                } else {
                    passTime = ToolUtils.createPassTime(date, ToolUtils.getDay(date, 1));
                }
                long recordID = ToolUtils.createRecordID(passTime);
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
