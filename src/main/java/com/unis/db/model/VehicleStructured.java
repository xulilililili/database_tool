package com.unis.db.model;

/**
 * @author xuli
 * @date 2019/2/12
 */
public class VehicleStructured {

    /**
     * 数据唯一标识
     */
    private Long recordID;
    /**
     * 车辆标识
     */
    private String motorVehicleID;
    /**
     * 关联卡口编号
     */
    private String tollgateID;
    /**
     * 设备编码
     */
    private String deviceID;
    /**
     * 近景照片
     */
    private String storageUrlCloseShot;
    /**
     * 车牌照片
     */
    private String storageUrlPlate;
    /**
     * 远景照片
     */
    private String storageUrlDistantShot;
    /**
     * 合成图
     */
    private String storageUrlCompound;
    /**
     * 缩略图
     */
    private String storageUrlBreviary;
    /**
     * 车道号，车辆行驶方向最左车道为1，由左向右顺序编号
     */
    private Integer laneNo;
    /**
     * 有无车牌，0：无车牌；1：有车牌
     */
    private Integer hasPlate;
    /**
     * 号牌种类
     */
    private Integer plateClass;
    /**
     * 车牌颜色
     */
    private Integer plateColor;
    /**
     * 车牌号码
     */
    private String plateNo;
    /**
     * 行驶速度，单位千米每小时
     */
    private Double speed;
    /**
     * 行驶方向
     */
    private Integer direction;
    /**
     * 行驶状态代码
     */
    private String drivingStatusCode;
    /**
     * 车辆左上角x坐标
     */
    private Double vehicleLeftTopX;
    /**
     * 车辆左上角y坐标
     */
    private Double vehicleLeftTopY;
    /**
     * 车辆上右下角x坐标
     */
    private Double vehicleRightBtmX;
    /**
     * 车辆上右下角y坐标
     */
    private Double vehicleRightBtmY;
    /**
     * 车辆类型
     */
    private String vehicleClass;
    /**
     * 车辆品牌
     */
    private Integer vehicleBrand;
    /**
     * 车辆型号
     */
    private String vehicleModel;
    /**
     * 车辆年款
     */
    private String vehicleStyles;
    /**
     * 车身颜色
     */
    private Integer vehicleColor;
    /**
     * 颜色深浅
     */
    private Integer vehicleColorDepth;
    /**
     * 过车时间
     */
    private Long passTime;
    /**
     * 视频中车龄出现时间
     */
    private Long vehicleAppearTime;
    /**
     * 视频中车辆消失时间
     */
    private Long vehicleDisappearTime;
    /**
     * 整个号牌号码的识别可信度
     */
    private Integer plateReliability;
    /**
     * 每位号牌号码可信度
     */
    private Integer plateCharReliability;
    /**
     * 车辆品牌标志可信度
     */
    private Integer brandReliability;
    /**
     * 驾驶员面部特征图
     */
    private String driverFace;
    /**
     * 副驾驶面部特征图
     */
    private String viceDriverFace;
    /**
     * 遮阳板
     */
    private Integer sunVisor;
    /**
     * 安全带状态
     */
    private Integer safetyBelt;
    /**
     * 打电话状态
     */
    private Integer calling;
    /**
     * 0：车头；1：车尾；2：侧面
     */
    private Integer vehicleAttitude;
    /**
     * 是否有香水瓶(0：无；1：有)
     */
    private Integer isPerfumeBottle;
    /**
     * 是否有挂饰(0：无；1：有)
     */
    private Integer isOrnament;
    /**
     * 是否有纸巾盒(0：无；1：有)
     */
    private Integer isTissue;
    /**
     * 是否有年检标(0：无；1：有)
     */
    private Integer isInspectionMark;
    /**
     * 是否有备胎(0：无；1：有)
     */
    private Integer isSpareWheel;
    /**
     * 是否渣土车(0：否；1：是)
     */
    private Integer isMuckCar;
    /**
     * 是否危化品车 (0：不是危化品车；1：是危化品车)
     */
    private Integer isHazardousTanker;
    /**
     * 是否渣土车(0：否；1：是)
     */
    private Integer isSunroof;
    /**
     * 车前部物品，当有多个时可用英文半角逗号分隔，见附录
     */
    private String vehicleFrontItem;
    /**
     * 对车前部物品数量、颜色、种类等信息描述
     */
    private String descOfFrontItem;
    /**
     * 0:没有撞痕；1:少量撞痕；2:大量撞痕
     */
    private Integer hitMarkInfo;
    /**
     * 对后视镜的描述
     */
    private String rearviewMirror;
    /**
     * 挂车车牌号
     */
    private String plateNoAttach;
    /**
     * 车窗描述
     */
    private String vehicleWindow;
    /**
     * 是否有行李架(0：无；1：有)
     */
    private Integer isLuggageRack;
    /**
     * 副驾驶遮阳板状态（0：收起； 1：放下）
     */
    private Integer coSunvisor;
    /**
     * 副驾驶安全带状态（0：未系； 1：有系）
     */
    private Integer coSafetyBelt;
    /**
     * 0：不是黄标车，1：是黄标车
     */
    private Integer isYellowLabel;

    public Integer getSunVisor() {
        return sunVisor;
    }

    public void setSunVisor(Integer sunVisor) {
        this.sunVisor = sunVisor;
    }

    public Integer getSafetyBelt() {
        return safetyBelt;
    }

    public void setSafetyBelt(Integer safetyBelt) {
        this.safetyBelt = safetyBelt;
    }

    public Integer getCalling() {
        return calling;
    }

    public void setCalling(Integer calling) {
        this.calling = calling;
    }

    public Long getRecordID() {
        return recordID;
    }

    public void setRecordID(Long recordID) {
        this.recordID = recordID;
    }

    public String getMotorVehicleID() {
        return motorVehicleID;
    }

    public void setMotorVehicleID(String motorVehicleID) {
        this.motorVehicleID = motorVehicleID;
    }

    public String getTollgateID() {
        return tollgateID;
    }

    public void setTollgateID(String tollgateID) {
        this.tollgateID = tollgateID;
    }

    public String getDeviceID() {
        return deviceID;
    }

    public void setDeviceID(String deviceID) {
        this.deviceID = deviceID;
    }

    public String getStorageUrlCloseShot() {
        return storageUrlCloseShot;
    }

    public void setStorageUrlCloseShot(String storageUrlCloseShot) {
        this.storageUrlCloseShot = storageUrlCloseShot;
    }

    public String getStorageUrlPlate() {
        return storageUrlPlate;
    }

    public void setStorageUrlPlate(String storageUrlPlate) {
        this.storageUrlPlate = storageUrlPlate;
    }

    public String getStorageUrlDistantShot() {
        return storageUrlDistantShot;
    }

    public void setStorageUrlDistantShot(String storageUrlDistantShot) {
        this.storageUrlDistantShot = storageUrlDistantShot;
    }

    public String getStorageUrlCompound() {
        return storageUrlCompound;
    }

    public void setStorageUrlCompound(String storageUrlCompound) {
        this.storageUrlCompound = storageUrlCompound;
    }

    public String getStorageUrlBreviary() {
        return storageUrlBreviary;
    }

    public void setStorageUrlBreviary(String storageUrlBreviary) {
        this.storageUrlBreviary = storageUrlBreviary;
    }

    public Integer getLaneNo() {
        return laneNo;
    }

    public void setLaneNo(Integer laneNo) {
        this.laneNo = laneNo;
    }

    public Integer getHasPlate() {
        return hasPlate;
    }

    public void setHasPlate(Integer hasPlate) {
        this.hasPlate = hasPlate;
    }

    public Integer getPlateClass() {
        return plateClass;
    }

    public void setPlateClass(Integer plateClass) {
        this.plateClass = plateClass;
    }

    public Integer getPlateColor() {
        return plateColor;
    }

    public void setPlateColor(Integer plateColor) {
        this.plateColor = plateColor;
    }

    public String getPlateNo() {
        return plateNo;
    }

    public void setPlateNo(String plateNo) {
        this.plateNo = plateNo;
    }

    public Double getSpeed() {
        return speed;
    }

    public void setSpeed(Double speed) {
        this.speed = speed;
    }

    public Integer getDirection() {
        return direction;
    }

    public void setDirection(Integer direction) {
        this.direction = direction;
    }

    public String getDrivingStatusCode() {
        return drivingStatusCode;
    }

    public void setDrivingStatusCode(String drivingStatusCode) {
        this.drivingStatusCode = drivingStatusCode;
    }

    public Double getVehicleLeftTopX() {
        return vehicleLeftTopX;
    }

    public void setVehicleLeftTopX(Double vehicleLeftTopX) {
        this.vehicleLeftTopX = vehicleLeftTopX;
    }

    public Double getVehicleLeftTopY() {
        return vehicleLeftTopY;
    }

    public void setVehicleLeftTopY(Double vehicleLeftTopY) {
        this.vehicleLeftTopY = vehicleLeftTopY;
    }

    public Double getVehicleRightBtmX() {
        return vehicleRightBtmX;
    }

    public void setVehicleRightBtmX(Double vehicleRightBtmX) {
        this.vehicleRightBtmX = vehicleRightBtmX;
    }

    public Double getVehicleRightBtmY() {
        return vehicleRightBtmY;
    }

    public void setVehicleRightBtmY(Double vehicleRightBtmY) {
        this.vehicleRightBtmY = vehicleRightBtmY;
    }

    public String getVehicleClass() {
        return vehicleClass;
    }

    public void setVehicleClass(String vehicleClass) {
        this.vehicleClass = vehicleClass;
    }

    public Integer getVehicleBrand() {
        return vehicleBrand;
    }

    public void setVehicleBrand(Integer vehicleBrand) {
        this.vehicleBrand = vehicleBrand;
    }

    public String getVehicleModel() {
        return vehicleModel;
    }

    public void setVehicleModel(String vehicleModel) {
        this.vehicleModel = vehicleModel;
    }

    public String getVehicleStyles() {
        return vehicleStyles;
    }

    public void setVehicleStyles(String vehicleStyles) {
        this.vehicleStyles = vehicleStyles;
    }

    public Integer getVehicleColor() {
        return vehicleColor;
    }

    public void setVehicleColor(Integer vehicleColor) {
        this.vehicleColor = vehicleColor;
    }

    public Integer getVehicleColorDepth() {
        return vehicleColorDepth;
    }

    public void setVehicleColorDepth(Integer vehicleColorDepth) {
        this.vehicleColorDepth = vehicleColorDepth;
    }

    public Long getPassTime() {
        return passTime;
    }

    public void setPassTime(Long passTime) {
        this.passTime = passTime;
    }

    public Long getVehicleAppearTime() {
        return vehicleAppearTime;
    }

    public void setVehicleAppearTime(Long vehicleAppearTime) {
        this.vehicleAppearTime = vehicleAppearTime;
    }

    public Long getVehicleDisappearTime() {
        return vehicleDisappearTime;
    }

    public void setVehicleDisappearTime(Long vehicleDisappearTime) {
        this.vehicleDisappearTime = vehicleDisappearTime;
    }

    public Integer getPlateReliability() {
        return plateReliability;
    }

    public void setPlateReliability(Integer plateReliability) {
        this.plateReliability = plateReliability;
    }

    public Integer getPlateCharReliability() {
        return plateCharReliability;
    }

    public void setPlateCharReliability(Integer plateCharReliability) {
        this.plateCharReliability = plateCharReliability;
    }

    public Integer getBrandReliability() {
        return brandReliability;
    }

    public void setBrandReliability(Integer brandReliability) {
        this.brandReliability = brandReliability;
    }

    public String getDriverFace() {
        return driverFace;
    }

    public void setDriverFace(String driverFace) {
        this.driverFace = driverFace;
    }

    public String getViceDriverFace() {
        return viceDriverFace;
    }

    public void setViceDriverFace(String viceDriverFace) {
        this.viceDriverFace = viceDriverFace;
    }

    public Integer getVehicleAttitude() {
        return vehicleAttitude;
    }

    public void setVehicleAttitude(Integer vehicleAttitude) {
        this.vehicleAttitude = vehicleAttitude;
    }

    public Integer getIsPerfumeBottle() {
        return isPerfumeBottle;
    }

    public void setIsPerfumeBottle(Integer isPerfumeBottle) {
        this.isPerfumeBottle = isPerfumeBottle;
    }

    public Integer getIsOrnament() {
        return isOrnament;
    }

    public void setIsOrnament(Integer isOrnament) {
        this.isOrnament = isOrnament;
    }

    public Integer getIsTissue() {
        return isTissue;
    }

    public void setIsTissue(Integer isTissue) {
        this.isTissue = isTissue;
    }

    public Integer getIsInspectionMark() {
        return isInspectionMark;
    }

    public void setIsInspectionMark(Integer isInspectionMark) {
        this.isInspectionMark = isInspectionMark;
    }

    public Integer getIsSpareWheel() {
        return isSpareWheel;
    }

    public void setIsSpareWheel(Integer isSpareWheel) {
        this.isSpareWheel = isSpareWheel;
    }

    public Integer getIsMuckCar() {
        return isMuckCar;
    }

    public void setIsMuckCar(Integer isMuckCar) {
        this.isMuckCar = isMuckCar;
    }

    public Integer getIsHazardousTanker() {
        return isHazardousTanker;
    }

    public void setIsHazardousTanker(Integer isHazardousTanker) {
        this.isHazardousTanker = isHazardousTanker;
    }

    public Integer getIsSunroof() {
        return isSunroof;
    }

    public void setIsSunroof(Integer isSunroof) {
        this.isSunroof = isSunroof;
    }

    public String getVehicleFrontItem() {
        return vehicleFrontItem;
    }

    public void setVehicleFrontItem(String vehicleFrontItem) {
        this.vehicleFrontItem = vehicleFrontItem;
    }

    public String getDescOfFrontItem() {
        return descOfFrontItem;
    }

    public void setDescOfFrontItem(String descOfFrontItem) {
        this.descOfFrontItem = descOfFrontItem;
    }

    public Integer getHitMarkInfo() {
        return hitMarkInfo;
    }

    public void setHitMarkInfo(Integer hitMarkInfo) {
        this.hitMarkInfo = hitMarkInfo;
    }

    public String getRearviewMirror() {
        return rearviewMirror;
    }

    public void setRearviewMirror(String rearviewMirror) {
        this.rearviewMirror = rearviewMirror;
    }

    public String getPlateNoAttach() {
        return plateNoAttach;
    }

    public void setPlateNoAttach(String plateNoAttach) {
        this.plateNoAttach = plateNoAttach;
    }

    public String getVehicleWindow() {
        return vehicleWindow;
    }

    public void setVehicleWindow(String vehicleWindow) {
        this.vehicleWindow = vehicleWindow;
    }

    public Integer getIsLuggageRack() {
        return isLuggageRack;
    }

    public void setIsLuggageRack(Integer isLuggageRack) {
        this.isLuggageRack = isLuggageRack;
    }

    public Integer getCoSunvisor() {
        return coSunvisor;
    }

    public void setCoSunvisor(Integer coSunvisor) {
        this.coSunvisor = coSunvisor;
    }

    public Integer getCoSafetyBelt() {
        return coSafetyBelt;
    }

    public void setCoSafetyBelt(Integer coSafetyBelt) {
        this.coSafetyBelt = coSafetyBelt;
    }

    public Integer getIsYellowLabel() {
        return isYellowLabel;
    }

    public void setIsYellowLabel(Integer isYellowLabel) {
        this.isYellowLabel = isYellowLabel;
    }

    @Override
    public String toString() {
        return recordID +
                "," + motorVehicleID +
                "," + tollgateID +
                "," + deviceID +
                "," + storageUrlCloseShot +
                "," + storageUrlPlate +
                "," + storageUrlDistantShot +
                "," + storageUrlCompound +
                "," + storageUrlBreviary +
                "," + laneNo +
                "," + hasPlate +
                "," + plateClass +
                "," + plateColor +
                "," + plateNo +
                "," + speed +
                "," + direction +
                "," + drivingStatusCode +
                "," + vehicleLeftTopX +
                "," + vehicleLeftTopY +
                "," + vehicleRightBtmX +
                "," + vehicleRightBtmY +
                "," + vehicleClass +
                "," + vehicleBrand +
                "," + vehicleModel +
                "," + vehicleStyles +
                "," + vehicleColor +
                "," + vehicleColorDepth +
                "," + passTime +
                "," + vehicleAppearTime +
                "," + vehicleDisappearTime +
                "," + plateReliability +
                "," + plateCharReliability +
                "," + brandReliability +
                "," + driverFace +
                "," + viceDriverFace +
                "," + sunVisor +
                "," + safetyBelt +
                "," + calling +
                "," + vehicleAttitude +
                "," + isPerfumeBottle +
                "," + isOrnament +
                "," + isTissue +
                "," + isInspectionMark +
                "," + isSpareWheel +
                "," + isMuckCar +
                "," + isHazardousTanker +
                "," + isSunroof +
                "," + vehicleFrontItem +
                "," + descOfFrontItem +
                "," + hitMarkInfo +
                "," + rearviewMirror +
                "," + plateNoAttach +
                "," + vehicleWindow +
                "," + isLuggageRack +
                "," + coSunvisor +
                "," + coSafetyBelt +
                "," + isYellowLabel;
    }
}
