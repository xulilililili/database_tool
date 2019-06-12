package com.unis.db.controller.dto;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * @author xuli
 * @date 2019/6/10
 */
public class QueryTaskByConditions {
    /**
     * 类型:车辆、人体、动态人脸
     */
    @NotNull(message = "类型为空", groups = {QueryTaskGroup.class})
    private String type;

    /**
     * 算法版本
     */
    private String algorithm = "a050200";

    /**
     * 线程数
     */
    @Max(message = "最大值不得大于16", groups = {QueryTaskGroup.class}, value = 16)
    @Min(message = "最小值不得小于1", groups = {QueryTaskGroup.class}, value = 1)
    @NotNull(message = "线程数为空", groups = {QueryTaskGroup.class})
    private int threadNum;

    /**
     * 每次查询任务时间间隔
     */
    @NotNull(message = "时间间隔为空", groups = {QueryTaskGroup.class})
    private int timeInterval = 60;

    /**
     * 留存期
     */
    private int remainDate = 150;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    public int getThreadNum() {
        return threadNum;
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    public int getTimeInterval() {
        return timeInterval;
    }

    public void setTimeInterval(int timeInterval) {
        this.timeInterval = timeInterval;
    }

    public int getRemainDate() {
        return remainDate;
    }

    public void setRemainDate(int remainDate) {
        this.remainDate = remainDate;
    }

    public interface QueryTaskGroup {
    }
}
