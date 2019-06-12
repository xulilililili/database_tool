package com.unis.db.controller;

        import com.unis.db.common.utils.DateUtils;
        import com.unis.db.common.utils.RandomUtils;
        import com.unis.db.controller.dto.QueryTaskByConditions;
        import com.unis.db.controller.dto.UseByConditions;
        import com.unis.db.service.TypeService;
        import com.unis.db.service.VehicleService;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;
        import org.springframework.beans.factory.annotation.Autowired;
        import org.springframework.validation.BindingResult;
        import org.springframework.validation.annotation.Validated;
        import org.springframework.web.bind.annotation.*;

/**
 * @author xuli
 * @date 2019/4/29
 */
@RestController
@RequestMapping(value = "/v1/search")
public class SearchController {

    private static final Logger logger = LoggerFactory.getLogger(SearchController.class);

    @Autowired
    private TypeService typeService;

    @Autowired
    private VehicleService vehicleService;

    @GetMapping(value = {"/recordId/vehicle"})
    public double getRecordIdRandom(@RequestParam(name = "month") String month,
                                    @RequestParam(name = "days", required = false, defaultValue = "1") int days) {
        String tableName = String.format("viid_vehicle.vehiclestructured_%s_%s", "a050200", month);
        String startDate = RandomUtils.getRandomDateInMonth(month, days);
        String endDate = DateUtils.getDateByAdd(startDate, days - 1);
        return typeService.getRecordIdPartition(tableName, startDate, endDate);
    }

    @GetMapping(value = {"/structured/vehicle"})
    public double searchStructured(@RequestParam(name = "month") String month,
                                   @RequestParam(name = "days", required = false, defaultValue = "1") int days,
                                   @RequestParam(name = "number", required = false, defaultValue = "1") int number) {
        String tableName = String.format("viid_vehicle.vehiclestructured_%s_%s", "a050200", month);
        String startDate = RandomUtils.getRandomDateInMonth(month, days);
        String endDate = DateUtils.getDateByAdd(startDate, days - 1);
        return vehicleService.searchVehicleStructured(tableName, startDate, endDate, number);
    }

    @PostMapping(value = {"/vehicle/query-task"})
    public Boolean queryTask(
            @Validated({QueryTaskByConditions.QueryTaskGroup.class}) @RequestBody QueryTaskByConditions queryTaskByConditions,
            BindingResult bindingResult) {
        MakeDataController.bindingResultCheck(bindingResult);
        return typeService.queryTask(queryTaskByConditions);
    }
}
