package com.unis.db.service;

/**
 * @author xuli
 * @date 2019/5/9
 */
public interface TerminalFeatureService {
    /**
     * 生成Wifi终端数据
     * @param passTime 时间戳
     * @param recordID 主键
     * @param partitionState 是否分区
     * @return 数据
     */
    String makeTerminalFeatureData(long passTime, long recordID, boolean partitionState);
}
