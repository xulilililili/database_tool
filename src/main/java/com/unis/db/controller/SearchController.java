package com.unis.db.controller;

import com.unis.db.common.utils.ToolUtils;
import com.unis.db.service.VehicleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author xuli
 * @date 2019/4/29
 */
@RestController
@RequestMapping(value = "/v1/search")
public class SearchController {

    private static final Logger logger = LoggerFactory.getLogger(SearchController.class);

    @Autowired
    private VehicleService vehicleService;

    @GetMapping(value = {"/recordId"})
    public double getRecordIdRandom(@RequestParam(name = "month") String month,
                                    @RequestParam(name = "days", required = false, defaultValue = "1") int days) {
        String tableName = String.format("viid_vehicle.vehiclestructured_%s_%s", "a050200", month);
        String startDate = ToolUtils.getDate(month, days);
        String endDate = ToolUtils.getDay(startDate, days - 1);
        return vehicleService.getRecordIdPartition(tableName, startDate, endDate);
    }

    @GetMapping(value = {"/structured"})
    public double searchStructured(@RequestParam(name = "month") String month,
                                   @RequestParam(name = "days", required = false, defaultValue = "1") int days,
                                   @RequestParam(name = "number", required = false, defaultValue = "1") int number) {
        String tableName = String.format("viid_vehicle.vehiclestructured_%s_%s", "a050200", month);
        String startDate = ToolUtils.getDate(month, days);
        String endDate = ToolUtils.getDay(startDate, days - 1);
        return vehicleService.searchStructured(tableName, startDate, endDate, number);
    }
}