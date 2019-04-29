package com.unis.db.controller.dto;

import org.springframework.format.annotation.DateTimeFormat;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * 数据库工具使用条件
 *
 * @author xuli
 * @date 2019/4/28
 */
public class UseByConditions {
    /**
     * 类型:车辆、人体、动态人脸
     */
    @NotNull(message = "类型为空", groups = {MakeDataGroup.class, SearchGroup.class})
    private String type;
    /**
     * 算法版本
     */
    @NotNull(message = "算法版本为空", groups = {MakeDataGroup.class, SearchGroup.class})
    private String algorithm;
    /**
     * 天数
     * 1.造数据代表造数据的天数
     * 2.查询代表跨越的天数
     */
    @NotNull(message = "天数为空", groups = {MakeDataGroup.class, SearchGroup.class})
    @Min(message = "最小值不得小于1", groups = {MakeDataGroup.class, SearchGroup.class}, value = 1)
    private int days;

    /**
     * 起始日期
     */
    @DateTimeFormat(pattern = "yyyyMMdd")
    @NotNull(message = "起始日期为空", groups = {MakeDataGroup.class, SearchGroup.class})
    private String startDate;

    /**
     * 线程数
     */
    @Max(message = "最大值不得大于1", groups = {MakeDataGroup.class}, value = 8)
    @Min(message = "最小值不得小于1", groups = {MakeDataGroup.class}, value = 1)
    @NotNull(message = "线程数为空", groups = {MakeDataGroup.class})
    private int threadNum;
    /**
     * 循环次数
     */
    @NotNull(message = "循环次数为空", groups = {MakeDataGroup.class})
    private int loop;
    /**
     * copyIn 单次提交条数
     */
    @NotNull(message = "单次提交条数为空", groups = {MakeDataGroup.class})
    private int batchSize;
    /**
     * 留存期
     */
    private int remainDate = 150;
    /**
     * 建表是否有索引:true false
     */
    private boolean index = false;

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

    public int getDays() {
        return days;
    }

    public void setDays(int days) {
        this.days = days;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public int getLoop() {
        return loop;
    }

    public void setLoop(int loop) {
        this.loop = loop;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getRemainDate() {
        return remainDate;
    }

    public void setRemainDate(int remainDate) {
        this.remainDate = remainDate;
    }

    public boolean isIndex() {
        return index;
    }

    public void setIndex(boolean index) {
        this.index = index;
    }

    public interface MakeDataGroup {
    }

    public interface SearchGroup {
    }
}
