package com.unis.db.model;

/**
 * @author xuli
 * @date 2019/5/9
 * 终端特征信息
 */
public class TerminalFeature {
    /**
     * 数据唯一标识
     */
    private Long recordID;
    /**
     * 终端mac地址
     */
    private String mac;
    /**
     * 终端品牌
     */
    private String brand;
    /**
     * 终端历史SSID列表
     */
    private String cacheSsid;
    /**
     * 采集时间
     */
    private Long captureTime;
    /**
     * 被采终端场强
     */
    private String terminalFieldStrength;
    /**
     * 身份类型
     */
    private Integer identificationType;
    /**
     * 身份内容
     */
    private String certificateCode;
    /**
     * 接入热点SSID
     */
    private String ssidPosition;
    /**
     * 接入热点mac
     */
    private String accessApMac;
    /**
     * 接入热点频道
     */
    private String accessApChannel;
    /**
     * 接入热点加密类型
     */
    private String accessApEncryptionType;
    /**
     * 终端相对采集设备的x坐标
     */
    private String xCoordinate;
    /**
     * 终端相对采集设备的y坐标
     */
    private String yCoordinate;
    /**
     * 场所编号
     */
    private String netbarWacode;
    /**
     * 采集设备编号
     */
    private String collectionEquipmentID;
    /**
     * 采集设备经度
     */
    private String collectionEquipmentLongitude;
    /**
     * 采集设备纬度
     */
    private String collectionEquipmentLatitude;

    public Long getRecordID() {
        return recordID;
    }

    public void setRecordID(Long recordID) {
        this.recordID = recordID;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getCacheSsid() {
        return cacheSsid;
    }

    public void setCacheSsid(String cacheSsid) {
        this.cacheSsid = cacheSsid;
    }

    public Long getCaptureTime() {
        return captureTime;
    }

    public void setCaptureTime(Long captureTime) {
        this.captureTime = captureTime;
    }

    public String getTerminalFieldStrength() {
        return terminalFieldStrength;
    }

    public void setTerminalFieldStrength(String terminalFieldStrength) {
        this.terminalFieldStrength = terminalFieldStrength;
    }

    public Integer getIdentificationType() {
        return identificationType;
    }

    public void setIdentificationType(Integer identificationType) {
        this.identificationType = identificationType;
    }

    public String getCertificateCode() {
        return certificateCode;
    }

    public void setCertificateCode(String certificateCode) {
        this.certificateCode = certificateCode;
    }

    public String getSsidPosition() {
        return ssidPosition;
    }

    public void setSsidPosition(String ssidPosition) {
        this.ssidPosition = ssidPosition;
    }

    public String getAccessApMac() {
        return accessApMac;
    }

    public void setAccessApMac(String accessApMac) {
        this.accessApMac = accessApMac;
    }

    public String getAccessApChannel() {
        return accessApChannel;
    }

    public void setAccessApChannel(String accessApChannel) {
        this.accessApChannel = accessApChannel;
    }

    public String getAccessApEncryptionType() {
        return accessApEncryptionType;
    }

    public void setAccessApEncryptionType(String accessApEncryptionType) {
        this.accessApEncryptionType = accessApEncryptionType;
    }

    public String getxCoordinate() {
        return xCoordinate;
    }

    public void setxCoordinate(String xCoordinate) {
        this.xCoordinate = xCoordinate;
    }

    public String getyCoordinate() {
        return yCoordinate;
    }

    public void setyCoordinate(String yCoordinate) {
        this.yCoordinate = yCoordinate;
    }

    public String getNetbarWacode() {
        return netbarWacode;
    }

    public void setNetbarWacode(String netbarWacode) {
        this.netbarWacode = netbarWacode;
    }

    public String getCollectionEquipmentID() {
        return collectionEquipmentID;
    }

    public void setCollectionEquipmentID(String collectionEquipmentID) {
        this.collectionEquipmentID = collectionEquipmentID;
    }

    public String getCollectionEquipmentLongitude() {
        return collectionEquipmentLongitude;
    }

    public void setCollectionEquipmentLongitude(String collectionEquipmentLongitude) {
        this.collectionEquipmentLongitude = collectionEquipmentLongitude;
    }

    public String getCollectionEquipmentLatitude() {
        return collectionEquipmentLatitude;
    }

    public void setCollectionEquipmentLatitude(String collectionEquipmentLatitude) {
        this.collectionEquipmentLatitude = collectionEquipmentLatitude;
    }

    @Override
    public String toString() {
        return  recordID +
                "," + mac  +
                "," + brand +
                "," + cacheSsid +
                "," + captureTime +
                "," + terminalFieldStrength +
                "," + identificationType +
                "," + certificateCode +
                "," + ssidPosition +
                "," + accessApMac +
                "," + accessApChannel +
                "," + accessApEncryptionType +
                "," + xCoordinate +
                "," + yCoordinate +
                "," + netbarWacode +
                "," + collectionEquipmentID +
                "," + collectionEquipmentLongitude +
                "," + collectionEquipmentLatitude;
    }
}
