package com.unis.db.model;

/**
 * @author xuli
 * @date 2019/4/22
 */
public class PersonStructured {
    /**
     * 数据唯一标识
     */
    private Long recordID;
    /**
     * 人员标识
     */
    private String personID;
    /**
     * 设备编码
     */
    private String deviceID;
    /**
     * 抓拍时间
     */
    private Long snapTimestamp;
    /**
     * 人员左上角x坐标
     */
    private Double leftTopX;
    /**
     * 人员左上角y坐标
     */
    private Double leftTopY;
    /**
     * 人员右下角x坐标
     */
    private Double rightBtmX;
    /**
     * 人员右下角y坐标
     */
    private Double rightBtmY;
    /**
     * 人员出现时间
     */
    private Long personAppearTime;
    /**
     * 人员消失时间
     */
    private Long personDisappearTime;
    /**
     * 性别代码，见附录
     */
    private Integer genderCode;
    /**
     * 年龄上限
     */
    private Integer ageUpLimit;
    /**
     * 年龄下限
     */
    private Integer ageLowerLimit;
    /**
     * 民族代码，见附录
     */
    private Integer ethicCode;
    /**
     * 同行人脸数
     */
    private Integer accompanyNumber;
    /**
     * 肤色
     */
    private Integer skinColor;
    /**
     * 发型
     */
    private Integer hairStyle;
    /**
     * 发色，见附录
     */
    private Integer hairColor;
    /**
     * 脸型
     */
    private Integer faceStyle;
    /**
     * 脸部特征
     */
    private String facialFeature;
    /**
     * 体貌特征
     */
    private String physicalFeature;
    /**
     * 伞颜色
     */
    private Integer umbrellaColor;
    /**
     * 口罩颜色
     */
    private Integer respiratorColor;
    /**
     * 帽子款式
     */
    private Integer capStyle;
    /**
     * 帽子颜色
     */
    private Integer capColor;
    /**
     * 眼镜款式
     */
    private Integer glassStyle;
    /**
     * 眼镜颜色
     */
    private Integer glassColor;
    /**
     * 围巾颜色
     */
    private Integer scarfColor;
    /**
     * 包款式
     */
    private Integer bagStyle;
    /**
     * 包颜色
     */
    private Integer bagColor;
    /**
     * 上衣款式
     */
    private Integer coatStyle;
    /**
     * 上衣颜色
     */
    private Integer coatColor;
    /**
     * 下衣款式
     */
    private Integer trousersStyle;
    /**
     * 下衣颜色
     */
    private Integer trousersColor;
    /**
     * 鞋子款式
     */
    private Integer shoesStyle;
    /**
     * 鞋子颜色
     */
    private Integer shoesColor;
    /**
     * 身高上限
     */
    private Integer heightUpLimit;
    /**
     * 身高下限
     */
    private Integer heightLowerLimit;
    /**
     * 体型
     */
    private Integer bodyType;
    /**
     * 姿态
     */
    private Integer gesture;
    /**
     * 状态
     */
    private Integer status;
    /**
     * 体表特征
     */
    private Integer bodyFeature;
    /**
     * 习惯动作
     */
    private Integer habitualMovement;
    /**
     * 行为
     */
    private Integer behavior;
    /**
     * 行为描述
     */
    private String behaviorDescription;
    /**
     * 附属物
     */
    private Integer appendant;
    /**
     * 附属物描述
     */
    private String appendantDescription;
    /**
     * 上衣长度
     */
    private Integer coatLength;
    /**
     * 裤子长度
     */
    private Integer trousersLen;
    /**
     * 是否驾驶员
     */
    private Integer isDriver;
    /**
     * 车辆类型
     */
    private Integer vehicleClass;
    /**
     * 是否涉外人员
     */
    private Integer isForeigner;
    /**
     * 护照证件种类
     */
    private Integer passportType;
    /**
     * 出入境人员类别代码
     */
    private String immigrantTypeCode;
    /**
     * 体表特殊标记
     */
    private Integer bodySpecialMark;
    /**
     * 图片质量
     */
    private Integer imageReliability;
    /**
     * 图片url
     */
    private String imageUrl;
    /**
     * 图片url部分
     */
    private String imageUrlPart;
    /**
     * 更新时间
     */
    private Long storageTime;
    /**
     * 对应同一个人脸的recordID
     */
    private Long extendID;
    /**
     * 0：长发；1：短发；2：无发；3：未识别
     */
    private Integer hairLen;
    /**
     * 上衣纹理，见附录
     */
    private Integer coatGrain;
    /**
     * 是否抱小孩(0:否，1：是)
     */
    private Integer isHoldBaby;
    /**
     * 是否背小孩(0:否，1：是)
     */
    private Integer isCarryBaby;
    /**
     * 是否手推车(0：无；1：有)
     */
    private Integer isHandcart;
    /**
     * 是否拎东西(0：未拎东西；1：拎东西)
     */
    private Integer isCarryThings;
    /**
     * 是否打伞(0：未打伞；1：打伞)
     */
    private Integer isUmbrellaOpen;
    /**
     * 是否戴口罩，0：未戴；1：戴
     */
    private Integer isRespirator;
    /**
     * 是否戴眼镜，0：未戴；1：戴
     */
    private Integer isGlasses;
    /**
     * 是否戴帽子，0：未戴；1：戴
     */
    private Integer isCap;
    /**
     * 朝向，见附录
     */
    private Integer orientation;
    /**
     * 可能有多个包，不同的包类型
     */
    private Long bagsStyle;
    /**
     * 可能有多个包，是否背某些包
     */
    private Long bagsColor;

    public Long getRecordID() {
        return recordID;
    }

    public void setRecordID(Long recordID) {
        this.recordID = recordID;
    }

    public String getPersonID() {
        return personID;
    }

    public void setPersonID(String personID) {
        this.personID = personID;
    }

    public String getDeviceID() {
        return deviceID;
    }

    public void setDeviceID(String deviceID) {
        this.deviceID = deviceID;
    }

    public Long getSnapTimestamp() {
        return snapTimestamp;
    }

    public void setSnapTimestamp(Long snapTimestamp) {
        this.snapTimestamp = snapTimestamp;
    }

    public Double getLeftTopX() {
        return leftTopX;
    }

    public void setLeftTopX(Double leftTopX) {
        this.leftTopX = leftTopX;
    }

    public Double getLeftTopY() {
        return leftTopY;
    }

    public void setLeftTopY(Double leftTopY) {
        this.leftTopY = leftTopY;
    }

    public Double getRightBtmX() {
        return rightBtmX;
    }

    public void setRightBtmX(Double rightBtmX) {
        this.rightBtmX = rightBtmX;
    }

    public Double getRightBtmY() {
        return rightBtmY;
    }

    public void setRightBtmY(Double rightBtmY) {
        this.rightBtmY = rightBtmY;
    }

    public Long getPersonAppearTime() {
        return personAppearTime;
    }

    public void setPersonAppearTime(Long personAppearTime) {
        this.personAppearTime = personAppearTime;
    }

    public Long getPersonDisappearTime() {
        return personDisappearTime;
    }

    public void setPersonDisappearTime(Long personDisappearTime) {
        this.personDisappearTime = personDisappearTime;
    }

    public Integer getGenderCode() {
        return genderCode;
    }

    public void setGenderCode(Integer genderCode) {
        this.genderCode = genderCode;
    }

    public Integer getAgeUpLimit() {
        return ageUpLimit;
    }

    public void setAgeUpLimit(Integer ageUpLimit) {
        this.ageUpLimit = ageUpLimit;
    }

    public Integer getAgeLowerLimit() {
        return ageLowerLimit;
    }

    public void setAgeLowerLimit(Integer ageLowerLimit) {
        this.ageLowerLimit = ageLowerLimit;
    }

    public Integer getEthicCode() {
        return ethicCode;
    }

    public void setEthicCode(Integer ethicCode) {
        this.ethicCode = ethicCode;
    }

    public Integer getAccompanyNumber() {
        return accompanyNumber;
    }

    public void setAccompanyNumber(Integer accompanyNumber) {
        this.accompanyNumber = accompanyNumber;
    }

    public Integer getSkinColor() {
        return skinColor;
    }

    public void setSkinColor(Integer skinColor) {
        this.skinColor = skinColor;
    }

    public Integer getHairStyle() {
        return hairStyle;
    }

    public void setHairStyle(Integer hairStyle) {
        this.hairStyle = hairStyle;
    }

    public Integer getHairColor() {
        return hairColor;
    }

    public void setHairColor(Integer hairColor) {
        this.hairColor = hairColor;
    }

    public Integer getFaceStyle() {
        return faceStyle;
    }

    public void setFaceStyle(Integer faceStyle) {
        this.faceStyle = faceStyle;
    }

    public String getFacialFeature() {
        return facialFeature;
    }

    public void setFacialFeature(String facialFeature) {
        this.facialFeature = facialFeature;
    }

    public String getPhysicalFeature() {
        return physicalFeature;
    }

    public void setPhysicalFeature(String physicalFeature) {
        this.physicalFeature = physicalFeature;
    }

    public Integer getUmbrellaColor() {
        return umbrellaColor;
    }

    public void setUmbrellaColor(Integer umbrellaColor) {
        this.umbrellaColor = umbrellaColor;
    }

    public Integer getRespiratorColor() {
        return respiratorColor;
    }

    public void setRespiratorColor(Integer respiratorColor) {
        this.respiratorColor = respiratorColor;
    }

    public Integer getCapStyle() {
        return capStyle;
    }

    public void setCapStyle(Integer capStyle) {
        this.capStyle = capStyle;
    }

    public Integer getCapColor() {
        return capColor;
    }

    public void setCapColor(Integer capColor) {
        this.capColor = capColor;
    }

    public Integer getGlassStyle() {
        return glassStyle;
    }

    public void setGlassStyle(Integer glassStyle) {
        this.glassStyle = glassStyle;
    }

    public Integer getGlassColor() {
        return glassColor;
    }

    public void setGlassColor(Integer glassColor) {
        this.glassColor = glassColor;
    }

    public Integer getScarfColor() {
        return scarfColor;
    }

    public void setScarfColor(Integer scarfColor) {
        this.scarfColor = scarfColor;
    }

    public Integer getBagStyle() {
        return bagStyle;
    }

    public void setBagStyle(Integer bagStyle) {
        this.bagStyle = bagStyle;
    }

    public Integer getBagColor() {
        return bagColor;
    }

    public void setBagColor(Integer bagColor) {
        this.bagColor = bagColor;
    }

    public Integer getCoatStyle() {
        return coatStyle;
    }

    public void setCoatStyle(Integer coatStyle) {
        this.coatStyle = coatStyle;
    }

    public Integer getCoatColor() {
        return coatColor;
    }

    public void setCoatColor(Integer coatColor) {
        this.coatColor = coatColor;
    }

    public Integer getTrousersStyle() {
        return trousersStyle;
    }

    public void setTrousersStyle(Integer trousersStyle) {
        this.trousersStyle = trousersStyle;
    }

    public Integer getTrousersColor() {
        return trousersColor;
    }

    public void setTrousersColor(Integer trousersColor) {
        this.trousersColor = trousersColor;
    }

    public Integer getShoesStyle() {
        return shoesStyle;
    }

    public void setShoesStyle(Integer shoesStyle) {
        this.shoesStyle = shoesStyle;
    }

    public Integer getShoesColor() {
        return shoesColor;
    }

    public void setShoesColor(Integer shoesColor) {
        this.shoesColor = shoesColor;
    }

    public Integer getHeightUpLimit() {
        return heightUpLimit;
    }

    public void setHeightUpLimit(Integer heightUpLimit) {
        this.heightUpLimit = heightUpLimit;
    }

    public Integer getHeightLowerLimit() {
        return heightLowerLimit;
    }

    public void setHeightLowerLimit(Integer heightLowerLimit) {
        this.heightLowerLimit = heightLowerLimit;
    }

    public Integer getBodyType() {
        return bodyType;
    }

    public void setBodyType(Integer bodyType) {
        this.bodyType = bodyType;
    }

    public Integer getGesture() {
        return gesture;
    }

    public void setGesture(Integer gesture) {
        this.gesture = gesture;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Integer getBodyFeature() {
        return bodyFeature;
    }

    public void setBodyFeature(Integer bodyFeature) {
        this.bodyFeature = bodyFeature;
    }

    public Integer getHabitualMovement() {
        return habitualMovement;
    }

    public void setHabitualMovement(Integer habitualMovement) {
        this.habitualMovement = habitualMovement;
    }

    public Integer getBehavior() {
        return behavior;
    }

    public void setBehavior(Integer behavior) {
        this.behavior = behavior;
    }

    public String getBehaviorDescription() {
        return behaviorDescription;
    }

    public void setBehaviorDescription(String behaviorDescription) {
        this.behaviorDescription = behaviorDescription;
    }

    public Integer getAppendant() {
        return appendant;
    }

    public void setAppendant(Integer appendant) {
        this.appendant = appendant;
    }

    public String getAppendantDescription() {
        return appendantDescription;
    }

    public void setAppendantDescription(String appendantDescription) {
        this.appendantDescription = appendantDescription;
    }

    public Integer getCoatLength() {
        return coatLength;
    }

    public void setCoatLength(Integer coatLength) {
        this.coatLength = coatLength;
    }

    public Integer getTrousersLen() {
        return trousersLen;
    }

    public void setTrousersLen(Integer trousersLen) {
        this.trousersLen = trousersLen;
    }

    public Integer getIsDriver() {
        return isDriver;
    }

    public void setIsDriver(Integer isDriver) {
        this.isDriver = isDriver;
    }

    public Integer getVehicleClass() {
        return vehicleClass;
    }

    public void setVehicleClass(Integer vehicleClass) {
        this.vehicleClass = vehicleClass;
    }

    public Integer getIsForeigner() {
        return isForeigner;
    }

    public void setIsForeigner(Integer isForeigner) {
        this.isForeigner = isForeigner;
    }

    public Integer getPassportType() {
        return passportType;
    }

    public void setPassportType(Integer passportType) {
        this.passportType = passportType;
    }

    public String getImmigrantTypeCode() {
        return immigrantTypeCode;
    }

    public void setImmigrantTypeCode(String immigrantTypeCode) {
        this.immigrantTypeCode = immigrantTypeCode;
    }

    public Integer getBodySpecialMark() {
        return bodySpecialMark;
    }

    public void setBodySpecialMark(Integer bodySpecialMark) {
        this.bodySpecialMark = bodySpecialMark;
    }

    public Integer getImageReliability() {
        return imageReliability;
    }

    public void setImageReliability(Integer imageReliability) {
        this.imageReliability = imageReliability;
    }

    public String getImageUrl() {
        return imageUrl;
    }

    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }

    public String getImageUrlPart() {
        return imageUrlPart;
    }

    public void setImageUrlPart(String imageUrlPart) {
        this.imageUrlPart = imageUrlPart;
    }

    public Long getStorageTime() {
        return storageTime;
    }

    public void setStorageTime(Long storageTime) {
        this.storageTime = storageTime;
    }

    public Long getExtendID() {
        return extendID;
    }

    public void setExtendID(Long extendID) {
        this.extendID = extendID;
    }

    public Integer getHairLen() {
        return hairLen;
    }

    public void setHairLen(Integer hairLen) {
        this.hairLen = hairLen;
    }

    public Integer getCoatGrain() {
        return coatGrain;
    }

    public void setCoatGrain(Integer coatGrain) {
        this.coatGrain = coatGrain;
    }

    public Integer getIsHoldBaby() {
        return isHoldBaby;
    }

    public void setIsHoldBaby(Integer isHoldBaby) {
        this.isHoldBaby = isHoldBaby;
    }

    public Integer getIsCarryBaby() {
        return isCarryBaby;
    }

    public void setIsCarryBaby(Integer isCarryBaby) {
        this.isCarryBaby = isCarryBaby;
    }

    public Integer getIsHandcart() {
        return isHandcart;
    }

    public void setIsHandcart(Integer isHandcart) {
        this.isHandcart = isHandcart;
    }

    public Integer getIsCarryThings() {
        return isCarryThings;
    }

    public void setIsCarryThings(Integer isCarryThings) {
        this.isCarryThings = isCarryThings;
    }

    public Integer getIsUmbrellaOpen() {
        return isUmbrellaOpen;
    }

    public void setIsUmbrellaOpen(Integer isUmbrellaOpen) {
        this.isUmbrellaOpen = isUmbrellaOpen;
    }

    public Integer getIsRespirator() {
        return isRespirator;
    }

    public void setIsRespirator(Integer isRespirator) {
        this.isRespirator = isRespirator;
    }

    public Integer getIsGlasses() {
        return isGlasses;
    }

    public void setIsGlasses(Integer isGlasses) {
        this.isGlasses = isGlasses;
    }

    public Integer getIsCap() {
        return isCap;
    }

    public void setIsCap(Integer isCap) {
        this.isCap = isCap;
    }

    public Integer getOrientation() {
        return orientation;
    }

    public void setOrientation(Integer orientation) {
        this.orientation = orientation;
    }

    public Long getBagsStyle() {
        return bagsStyle;
    }

    public void setBagsStyle(Long bagsStyle) {
        this.bagsStyle = bagsStyle;
    }

    public Long getBagsColor() {
        return bagsColor;
    }

    public void setBagsColor(Long bagsColor) {
        this.bagsColor = bagsColor;
    }

    @Override
    public String toString() {
        return recordID +
                "," + personID +
                "," + deviceID +
                "," + snapTimestamp +
                "," + leftTopX +
                "," + leftTopY +
                "," + rightBtmX +
                "," + rightBtmY +
                "," + personAppearTime +
                "," + personDisappearTime +
                "," + genderCode +
                "," + ageUpLimit +
                "," + ageLowerLimit +
                "," + ethicCode +
                "," + accompanyNumber +
                "," + skinColor +
                "," + hairStyle +
                "," + hairColor +
                "," + faceStyle +
                "," + facialFeature +
                "," + physicalFeature +
                "," + umbrellaColor +
                "," + respiratorColor +
                "," + capStyle +
                "," + capColor +
                "," + glassStyle +
                "," + glassColor +
                "," + scarfColor +
                "," + bagStyle +
                "," + bagColor +
                "," + coatStyle +
                "," + coatColor +
                "," + trousersStyle +
                "," + trousersColor +
                "," + shoesStyle +
                "," + shoesColor +
                "," + heightUpLimit +
                "," + heightLowerLimit +
                "," + bodyType +
                "," + gesture +
                "," + status +
                "," + bodyFeature +
                "," + habitualMovement +
                "," + behavior +
                "," + behaviorDescription +
                "," + appendant +
                "," + appendantDescription +
                "," + coatLength +
                "," + trousersLen +
                "," + isDriver +
                "," + vehicleClass +
                "," + isForeigner +
                "," + passportType +
                "," + immigrantTypeCode +
                "," + bodySpecialMark +
                "," + imageReliability +
                "," + imageUrl +
                "," + imageUrlPart +
                "," + storageTime +
                "," + extendID +
                "," + hairLen +
                "," + coatGrain +
                "," + isHoldBaby +
                "," + isCarryBaby +
                "," + isHandcart +
                "," + isCarryThings +
                "," + isUmbrellaOpen +
                "," + isRespirator +
                "," + isGlasses +
                "," + isCap +
                "," + orientation +
                "," + bagsStyle +
                "," + bagsColor;
    }
}
