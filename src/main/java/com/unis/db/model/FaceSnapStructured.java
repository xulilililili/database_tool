package com.unis.db.model;



/**
 * @author  xuli
 * @date 2019/3/22
 */
public class FaceSnapStructured {
    /**
     * 数据唯一标识
     */
    private Long recordID;
    /**
     * 人脸标识
     */
    private String faceID;
    /**
     * 设备编码
     */
    private String deviceID;
    /**
     * 关联卡口编号
     */
    private String tollgateID;
    /**
     * 人脸左上角x坐标
     */
    private Double leftTopX;
    /**
     * 人脸左上角y坐标
     */
    private Double leftTopY;
    /**
     * 人脸右下角x坐标
     */
    private Double rightBtmX;
    /**
     * 人脸右下角y坐标
     */
    private Double rightBtmY;
    /**
     * 小图url
     */
    private String imageUrlPart;
    /**
     * 全景图url
     */
    private String imageUrlFull;
    /**
     * 抓拍时间
     */
    private Long passTime;
    /**
     * 性别代码,见附录
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
     * 颜值
     */
    private Integer appearLevel;
    /**
     * 民族代码,见附录
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
     * 发色,见附录
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
     * 是否戴口罩,0：未戴；1：戴
     */
    private Integer isRespirator;
    /**
     * 口罩颜色,见附录
     */
    private Integer respiratorColor;
    /**
     * 是否戴帽子,0：未戴；1：戴
     */
    private Integer isCap;
    /**
     * 是否戴眼镜,0：未戴；1：戴
     */
    private Integer capStyle;
    /**
     * 帽子颜色,见附录
     */
    private Integer capColor;
    /**
     * 是否戴眼镜,0：未戴；1：戴
     */
    private Integer isGlasses;
    /**
     * 眼镜款式
     */
    private Integer glassesStyleType;
    /**
     * 眼镜颜色,见附录
     */
    private Integer glassColor;
    /**
     * 是否驾驶员,0：否；1：是；2：不确定
     */
    private Integer isDriver;
    /**
     * 眉型,1：上扬眉；2：一字眉；3：八字眉；4：淡眉毛
     */
    private Integer eyeBrowStyle;
    /**
     * 是否有胡子,0：没有；1:有
     */
    private Integer isBeard;
    /**
     * 鼻型,1：普通鼻；2：扁鼻子；3：尖鼻子；4：朝天鼻；5：鹰钩鼻；6：蒜头鼻；7：宽鼻子；8：小鼻子；9：露孔鼻；10：特殊鼻；
     */
    private Integer noseStyle;
    /**
     * 胡型,1：一字胡；2：八字胡；3：淡胡子；4：络腮胡；5：山羊胡；6：花白胡；7：一点胡
     */
    private Integer mustacheStyle;
    /**
     * 嘴唇,1：常见嘴；2：厚嘴唇；3：薄嘴唇；4：厚嘴巴；5：上翘嘴；6：下撇嘴；7：凸嘴；8：凹嘴；9：露齿嘴；10：小嘴
     */
    private Integer lipStyle;
    /**
     * 皱纹眼袋,1：抬头纹；2：左眼角纹；3：右眼角纹；4：眉间纹；5：左眼纹；6：右眼纹；7：眼袋；8：左笑纹；9：右笑纹；10：鼻间纹；11：左瘦纹；12：右瘦纹
     */
    private Integer wrinklePouch;
    /**
     * 痤疮色斑,1：痤疮（单）；2：痤疮（少）；3：痤疮（多）；4：雀斑（稀）；5：雀斑（少）；6：雀斑（多）；7：色斑
     */
    private Integer acneStain;
    /**
     * 黑痣胎记,1：痣（小）；2：痣（中）；3：痣（大）；4：黑痣（小）；5：黑痣（中）；6：黑痣（大）；7：胎记
     */
    private Integer freckleBirthMark;
    /**
     * 疤痕酒窝,1：疤痕；2：酒窝左；3：酒窝右
     */
    private Integer scarDimple;
    /**
     * 其他特征,1：酒渣鼻（小）；2：酒渣鼻（大）；3：酒渣鼻（重）；4：招风耳左；5：招风耳右；6：大耳朵左；7：大耳朵右；8：粗糙皮肤；9：白癜风小；10：白癜风中；11：白癜风大
     */
    private Integer otherFeature;
    /**
     * 是否微笑,0:否；1：是
     */
    private Integer isSmile;
    /**
     * 是否睁眼,0:否；1：是
     */
    private Integer isOpenEyes;
    /**
     * 是否张嘴,0:否；1：是
     */
    private Integer isOpenMouth;
    /**
     * 图片质量
     */
    private Integer imageReliability;
    /**
     * 是否经过ia处理, 0:已处理；1：未处理
     */
    private Integer isIaDisposed;
    /**
     * 对应同一个人体的recordID
     */
    private Long extendID;

    public Long getExtendID() {
        return extendID;
    }

    public void setExtendID(Long extendID) {
        this.extendID = extendID;
    }

    public Long getRecordID() {
        return recordID;
    }

    public void setRecordID(Long recordID) {
        this.recordID = recordID;
    }

    public String getFaceID() {
        return faceID;
    }

    public void setFaceID(String faceID) {
        this.faceID = faceID;
    }

    public String getDeviceID() {
        return deviceID;
    }

    public void setDeviceID(String deviceID) {
        this.deviceID = deviceID;
    }

    public String getTollgateID() {
        return tollgateID;
    }

    public void setTollgateID(String tollgateID) {
        this.tollgateID = tollgateID;
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

    public String getImageUrlPart() {
        return imageUrlPart;
    }

    public void setImageUrlPart(String imageUrlPart) {
        this.imageUrlPart = imageUrlPart;
    }

    public String getImageUrlFull() {
        return imageUrlFull;
    }

    public void setImageUrlFull(String imageUrlFull) {
        this.imageUrlFull = imageUrlFull;
    }

    public Long getPassTime() {
        return passTime;
    }

    public void setPassTime(Long passTime) {
        this.passTime = passTime;
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

    public Integer getAppearLevel() {
        return appearLevel;
    }

    public void setAppearLevel(Integer appearLevel) {
        this.appearLevel = appearLevel;
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

    public Integer getIsRespirator() {
        return isRespirator;
    }

    public void setIsRespirator(Integer isRespirator) {
        this.isRespirator = isRespirator;
    }

    public Integer getRespiratorColor() {
        return respiratorColor;
    }

    public void setRespiratorColor(Integer respiratorColor) {
        this.respiratorColor = respiratorColor;
    }

    public Integer getIsCap() {
        return isCap;
    }

    public void setIsCap(Integer isCap) {
        this.isCap = isCap;
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

    public Integer getIsGlasses() {
        return isGlasses;
    }

    public void setIsGlasses(Integer isGlasses) {
        this.isGlasses = isGlasses;
    }

    public Integer getGlassesStyleType() {
        return glassesStyleType;
    }

    public void setGlassesStyleType(Integer glassesStyleType) {
        this.glassesStyleType = glassesStyleType;
    }

    public Integer getGlassColor() {
        return glassColor;
    }

    public void setGlassColor(Integer glassColor) {
        this.glassColor = glassColor;
    }

    public Integer getIsDriver() {
        return isDriver;
    }

    public void setIsDriver(Integer isDriver) {
        this.isDriver = isDriver;
    }

    public Integer getEyeBrowStyle() {
        return eyeBrowStyle;
    }

    public void setEyeBrowStyle(Integer eyeBrowStyle) {
        this.eyeBrowStyle = eyeBrowStyle;
    }

    public Integer getIsBeard() {
        return isBeard;
    }

    public void setIsBeard(Integer isBeard) {
        this.isBeard = isBeard;
    }

    public Integer getNoseStyle() {
        return noseStyle;
    }

    public void setNoseStyle(Integer noseStyle) {
        this.noseStyle = noseStyle;
    }

    public Integer getMustacheStyle() {
        return mustacheStyle;
    }

    public void setMustacheStyle(Integer mustacheStyle) {
        this.mustacheStyle = mustacheStyle;
    }

    public Integer getLipStyle() {
        return lipStyle;
    }

    public void setLipStyle(Integer lipStyle) {
        this.lipStyle = lipStyle;
    }

    public Integer getWrinklePouch() {
        return wrinklePouch;
    }

    public void setWrinklePouch(Integer wrinklePouch) {
        this.wrinklePouch = wrinklePouch;
    }

    public Integer getAcneStain() {
        return acneStain;
    }

    public void setAcneStain(Integer acneStain) {
        this.acneStain = acneStain;
    }

    public Integer getFreckleBirthMark() {
        return freckleBirthMark;
    }

    public void setFreckleBirthMark(Integer freckleBirthMark) {
        this.freckleBirthMark = freckleBirthMark;
    }

    public Integer getScarDimple() {
        return scarDimple;
    }

    public void setScarDimple(Integer scarDimple) {
        this.scarDimple = scarDimple;
    }

    public Integer getOtherFeature() {
        return otherFeature;
    }

    public void setOtherFeature(Integer otherFeature) {
        this.otherFeature = otherFeature;
    }

    public Integer getIsSmile() {
        return isSmile;
    }

    public void setIsSmile(Integer isSmile) {
        this.isSmile = isSmile;
    }

    public Integer getIsOpenEyes() {
        return isOpenEyes;
    }

    public void setIsOpenEyes(Integer isOpenEyes) {
        this.isOpenEyes = isOpenEyes;
    }

    public Integer getIsOpenMouth() {
        return isOpenMouth;
    }

    public void setIsOpenMouth(Integer isOpenMouth) {
        this.isOpenMouth = isOpenMouth;
    }

    public Integer getImageReliability() {
        return imageReliability;
    }

    public void setImageReliability(Integer imageReliability) {
        this.imageReliability = imageReliability;
    }

    public Integer getIsIaDisposed() {
        return isIaDisposed;
    }

    public void setIsIaDisposed(Integer isIaDisposed) {
        this.isIaDisposed = isIaDisposed;
    }

    @Override
    public String toString() {
        return  recordID +
                "," + faceID +
                "," + deviceID +
                "," + tollgateID +
                "," + leftTopX +
                "," + leftTopY +
                "," + rightBtmX +
                "," + rightBtmY +
                "," + imageUrlPart +
                "," + imageUrlFull +
                "," + passTime +
                "," + genderCode +
                "," + ageUpLimit +
                "," + ageLowerLimit +
                "," + appearLevel +
                "," + ethicCode +
                "," + accompanyNumber +
                "," + skinColor +
                "," + hairStyle +
                "," + hairColor +
                "," + faceStyle +
                "," + facialFeature +
                "," + physicalFeature +
                "," + isRespirator +
                "," + respiratorColor +
                "," + isCap +
                "," + capStyle +
                "," + capColor +
                "," + isGlasses +
                "," + glassesStyleType +
                "," + glassColor +
                "," + isDriver +
                "," + eyeBrowStyle +
                "," + isBeard +
                "," + noseStyle +
                "," + mustacheStyle +
                "," + lipStyle +
                "," + wrinklePouch +
                "," + acneStain +
                "," + freckleBirthMark +
                "," + scarDimple +
                "," + otherFeature +
                "," + isSmile +
                "," + isOpenEyes +
                "," + isOpenMouth +
                "," + imageReliability +
                "," + isIaDisposed +
                "," + extendID;
    }
}
