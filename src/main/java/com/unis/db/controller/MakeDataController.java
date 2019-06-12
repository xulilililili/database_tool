package com.unis.db.controller;

import com.unis.db.controller.dto.UseByConditions;
import com.unis.db.service.MakeDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;


/**
 * @author xuli
 * @date 2019/4/16
 */
@RestController
@RequestMapping(value = "/v1")
public class MakeDataController {

    private static final Logger logger = LoggerFactory.getLogger(MakeDataController.class);

    @Autowired
    private MakeDataService makeDataService;

    @PostMapping(value = {"/make-data"})
    public Boolean makeData(
            @Validated({UseByConditions.MakeDataGroup.class}) @RequestBody UseByConditions useByConditions,
            BindingResult bindingResult) {
        bindingResultCheck(bindingResult);
        return makeDataService.makeData(useByConditions);
    }

    @PostMapping(value = {"/make-data/partition"})
    public Boolean makeDataByPartition(
            @Validated({UseByConditions.MakeDataGroup.class}) @RequestBody UseByConditions useByConditions,
            BindingResult bindingResult) {
        bindingResultCheck(bindingResult);
        return makeDataService.makeDataByPartition(useByConditions);
    }

    public static void bindingResultCheck(BindingResult bindingResult) {
        //捕获不合法参数
        if (bindingResult.hasErrors()) {
            // 得到全部不合法的对象
            List<ObjectError> objectErrorList = bindingResult.getAllErrors();
            for (ObjectError objectError : objectErrorList) {
                logger.error("error field is:{} ,message is : {}", ((FieldError) objectError).getField(), objectError.getDefaultMessage());
            }
        }
    }

}
