package com.unis.db.service.impl;

import com.unis.db.common.enums.PersonEnum;
import com.unis.db.common.utils.DateUtils;
import com.unis.db.model.PersonStructured;
import com.unis.db.service.PersonService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Random;

/**
 * @author xuli
 * @date 2019/4/22
 */
@Service
public class PersonServiceImpl implements PersonService {

    private static final Logger logger = LoggerFactory.getLogger(PersonServiceImpl.class);

    private PersonStructured person = new PersonStructured();

    private Random random = new Random();

    @Override
    public String makePersonData(long passTime, long recordID, boolean partitionState) {
        person.setRecordID(recordID);
        person.setPersonID(Integer.toString(random.nextInt(100000000)));
        person.setDeviceID(PersonEnum.DEVICE_PREFIX + (random.nextInt(100) + 400));
        person.setSnapTimestamp(passTime);
        person.setLeftTopX(0.0000);
        person.setLeftTopY(0.0000);
        person.setRightBtmX(0.0000);
        person.setRightBtmY(0.0000);
        person.setPersonAppearTime(passTime);
        person.setPersonDisappearTime(passTime + 1000);
        person.setGenderCode(PersonEnum.GENDER_TYPE[random.nextInt(4)]);
        person.setAgeUpLimit(random.nextInt(45) + 35);
        person.setAgeLowerLimit(person.getAgeUpLimit() - 15 - random.nextInt(10));
        person.setEthicCode(random.nextInt(56) + 1);
        person.setAccompanyNumber(random.nextInt(2) + 1);
        person.setSkinColor(1);
        person.setHairStyle(random.nextInt(12) + 1);
        person.setHairColor(PersonEnum.COLOR[random.nextInt(PersonEnum.COLOR.length)]);
        person.setFaceStyle(1);
        person.setFacialFeature("1");
        person.setPhysicalFeature("1");
        person.setUmbrellaColor(PersonEnum.COLOR[random.nextInt(PersonEnum.COLOR.length)]);
        person.setRespiratorColor(11);
        person.setCapStyle(1);
        person.setCapColor(PersonEnum.COLOR[random.nextInt(PersonEnum.COLOR.length)]);
        person.setGlassStyle(random.nextInt(10) + 1);
        person.setGlassColor(PersonEnum.COLOR[random.nextInt(PersonEnum.COLOR.length)]);
        person.setScarfColor(PersonEnum.COLOR[random.nextInt(PersonEnum.COLOR.length)]);
        person.setBagStyle(random.nextInt(13) + 1);
        person.setBagColor(PersonEnum.COLOR[random.nextInt(PersonEnum.COLOR.length)]);
        person.setCoatStyle(PersonEnum.COAT_STYLE[random.nextInt(PersonEnum.COAT_STYLE.length)]);
        person.setCoatColor(PersonEnum.COLOR[random.nextInt(PersonEnum.COLOR.length)]);
        person.setTrousersStyle(PersonEnum.TROUSERS_STYLE[random.nextInt(PersonEnum.TROUSERS_STYLE.length)]);
        person.setTrousersColor(PersonEnum.COLOR[random.nextInt(PersonEnum.COLOR.length)]);
        person.setShoesStyle(random.nextInt(13) + 1);
        person.setShoesColor(PersonEnum.COLOR[random.nextInt(PersonEnum.COLOR.length)]);
        person.setHeightUpLimit(0);
        person.setHeightLowerLimit(0);
        person.setBodyType(1);
        person.setGesture(random.nextInt(11) + 1);
        person.setStatus(random.nextInt(4) + 1);
        person.setBodyFeature(0);
        person.setHabitualMovement(random.nextInt(19) + 1);
        person.setBehavior(random.nextInt(7) + 1);
        person.setBehaviorDescription("1");
        person.setAppendant(random.nextInt(10) + 1);
        person.setAppendantDescription("1");
        person.setCoatLength(random.nextInt(3) + 1);
        person.setTrousersLen(random.nextInt(2) + 1);
        person.setIsDriver(random.nextInt(2));
        person.setVehicleClass(0);
        person.setIsForeigner(random.nextInt(2));
        person.setPassportType(random.nextInt(51) + 11);
        person.setImmigrantTypeCode("1");
        person.setBodySpecialMark(0);
        person.setImageReliability(0);
        person.setImageUrl("/picture/2018-08-17T08-29-20Z/1534495880605_3865.jpg");
        person.setImageUrlPart("1");
        person.setStorageTime(0L);
        person.setExtendID(0L);
        person.setHairLen(random.nextInt(4));
        person.setCoatGrain(PersonEnum.COAT_GRAIN[random.nextInt(PersonEnum.COAT_GRAIN.length)]);
        person.setIsHoldBaby(random.nextInt(2));
        person.setIsCarryBaby(random.nextInt(2));
        person.setIsHandcart(random.nextInt(2));
        person.setIsCarryThings(random.nextInt(2));
        person.setIsUmbrellaOpen(random.nextInt(2));
        person.setIsRespirator(random.nextInt(2));
        person.setIsGlasses(random.nextInt(2));
        person.setIsCap(random.nextInt(2));
        person.setOrientation(PersonEnum.ORIENTATION[random.nextInt(PersonEnum.ORIENTATION.length)]);
        person.setBagsStyle(0L);
        person.setBagsColor(0L);

        String data = person.toString();
        if (partitionState) {
            data = data + "," + DateUtils.longToString(person.getSnapTimestamp(), "yyyy-MM-dd");
        }
        return data;
    }
}
