package com.unis.db.service.impl;


import com.unis.db.common.utils.DateUtils;
import com.unis.db.model.FaceSnapStructured;
import com.unis.db.service.FaceSnapService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Random;

/**
 * @author xuli
 * @date 2019/4/18
 */
@Service
public class FaceSnapServiceImpl implements FaceSnapService {

    private static final Logger logger = LoggerFactory.getLogger(FaceSnapServiceImpl.class);
    /**
     * 人脸设备id 共108个
     * 例子:50010500001191000xxx
     */
    private static final String DEVICE_PREFIX = "50010500001191000";
    private static final String TOLLGATE_PREFIX = "50010500001211000";
    private static final String[] DEVICE_SUFFIX = {"088", "091", "191", "192", "193", "194", "195", "196", "197", "198", "199", "200", "201", "202", "203", "204", "205", "206", "207", "208", "209", "210", "211", "212", "213", "214", "215", "216", "217", "218", "219", "220", "221", "222", "223", "224", "225", "226", "227", "228", "229", "230", "231", "232", "233", "234", "235", "236", "237", "238", "239", "240", "241", "242", "243", "244", "245", "246", "247", "248", "249", "250", "251", "252", "253", "254", "255", "256", "257", "258", "259", "260", "261", "262", "263", "264", "265", "266", "267", "268", "269", "270", "271", "272", "273", "274", "275", "276", "277", "278", "279", "280", "281", "282", "283", "284", "285", "286", "287", "288", "289", "290", "291", "292", "293", "294", "295", "296"};
    /**
     * 性别代码
     */
    private static final int[] GENDER_TYPE = {0, 1, 2, 9};

    private FaceSnapStructured faceSnap = new FaceSnapStructured();

    private Random random = new Random();

    @Override
    public String makeFaceSnapData(long passTime, long recordID, boolean partitionState) {
        faceSnap.setRecordID(recordID);
        faceSnap.setFaceID(Integer.toString(random.nextInt(100000000)));
        faceSnap.setDeviceID(DEVICE_PREFIX + DEVICE_SUFFIX[random.nextInt(DEVICE_SUFFIX.length)]);
        faceSnap.setTollgateID(TOLLGATE_PREFIX + DEVICE_SUFFIX[random.nextInt(DEVICE_SUFFIX.length)]);
        faceSnap.setLeftTopX(0.0000);
        faceSnap.setLeftTopY(0.0000);
        faceSnap.setRightBtmX(0.0000);
        faceSnap.setRightBtmY(0.0000);
        faceSnap.setImageUrlPart("");
        faceSnap.setImageUrlFull("");
        faceSnap.setPassTime(passTime);
        faceSnap.setGenderCode(GENDER_TYPE[random.nextInt(4)]);
        faceSnap.setAgeUpLimit(random.nextInt(45) + 35);
        faceSnap.setAgeLowerLimit(faceSnap.getAgeUpLimit() - 15 - random.nextInt(10));
        faceSnap.setAppearLevel(random.nextInt(10));
        faceSnap.setEthicCode(random.nextInt(56) + 1);
        faceSnap.setAccompanyNumber(random.nextInt(2));
        faceSnap.setSkinColor(1);
        faceSnap.setHairStyle(1);
        faceSnap.setHairColor(1);
        faceSnap.setFaceStyle(1);
        faceSnap.setFacialFeature("");
        faceSnap.setPhysicalFeature("");
        faceSnap.setIsRespirator(random.nextInt(2));
        faceSnap.setRespiratorColor(1);
        faceSnap.setIsCap(random.nextInt(2));
        faceSnap.setCapStyle(1);
        faceSnap.setCapColor(1);
        faceSnap.setIsGlasses(random.nextInt(2));
        faceSnap.setGlassesStyleType(1);
        faceSnap.setGlassColor(1);
        faceSnap.setIsDriver(random.nextInt(3) + 1);
        faceSnap.setEyeBrowStyle(random.nextInt(4) + 1);
        faceSnap.setIsBeard(random.nextInt(2));
        faceSnap.setNoseStyle(random.nextInt(10) + 1);
        faceSnap.setMustacheStyle(random.nextInt(7) + 1);
        faceSnap.setLipStyle(random.nextInt(10) + 1);
        faceSnap.setWrinklePouch(random.nextInt(12) + 1);
        faceSnap.setAcneStain(random.nextInt(7) + 1);
        faceSnap.setFreckleBirthMark(random.nextInt(7) + 1);
        faceSnap.setScarDimple(random.nextInt(3) + 1);
        faceSnap.setOtherFeature(random.nextInt(11) + 1);
        faceSnap.setIsSmile(random.nextInt(2));
        faceSnap.setIsOpenEyes(random.nextInt(2));
        faceSnap.setIsOpenMouth(random.nextInt(2));
        faceSnap.setImageReliability(1);
        faceSnap.setIsIaDisposed(random.nextInt(2));
        faceSnap.setExtendID(0L);

        String data = faceSnap.toString();
        if (partitionState) {
            data = data + "," + DateUtils.longToString(faceSnap.getPassTime(), "yyyy-MM-dd");
        }
        return data;
    }
}
