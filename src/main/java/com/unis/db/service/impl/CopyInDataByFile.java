package com.unis.db.service.impl;

import com.unis.db.common.utils.CopyInUtils;
import com.unis.db.common.utils.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * @author xuli
 * @date 2019/3/22
 */
public class CopyInDataByFile implements Callable {
    private static final Logger logger = LoggerFactory.getLogger(CopyInDataByFile.class);

    private FaceSnapServiceImpl faceSnapService = new FaceSnapServiceImpl();

    private String tableName;

    private String filePath;

    public CopyInDataByFile(String tableName, String filePath) {
        this.tableName = tableName;
        this.filePath = filePath;
    }

    @Override
    public Boolean call() {
        List<String> dataList = new ArrayList<>(1000);
        String line;
        try {
            //使用缓冲区将数据读入到缓冲区中
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(new FileInputStream(filePath)));
            while ((line = reader.readLine()) != null) {
                List<String> recordIDList = Arrays.asList(line.split(","));
                for (String recordID : recordIDList) {
                    dataList.add(faceSnapService.makeFaceSnapData(RandomUtils.getPassTime(Long.parseLong(recordID)), Long.parseLong(recordID), false));
                }
                String data = String.join("\n", dataList);
                //执行copyIn
                if (!CopyInUtils.copyIn(data, tableName)) {
                    return false;
                }
                dataList.clear();
            }
            reader.close();
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }
}
