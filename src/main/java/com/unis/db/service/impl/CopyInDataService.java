package com.unis.db.service.impl;

import com.unis.db.common.utils.CopyInUtils;
import com.unis.db.common.utils.DateUtils;
import com.unis.db.common.utils.RandomUtils;
import com.unis.db.service.FaceSnapService;
import com.unis.db.service.PersonService;
import com.unis.db.service.TerminalFeatureService;
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

    private VehicleService vehicleService = new VehicleServiceImpl();

    private FaceSnapService faceSnapService = new FaceSnapServiceImpl();

    private PersonService personService = new PersonServiceImpl();

    private TerminalFeatureService terminalFeatureService = new TerminalFeatureServiceImpl();

    private String tableName;

    private String date;

    private String type;

    private int loop;

    private int batchSize;

    private boolean partitionState;

    public CopyInDataService(String tableName, String date, String type, int loop, int batchSize, boolean partitionState) {
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
