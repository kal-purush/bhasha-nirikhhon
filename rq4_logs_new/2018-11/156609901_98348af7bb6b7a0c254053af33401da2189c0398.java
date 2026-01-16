package edu.iss.gitcloneteam.weatherobserver.controllers;

import edu.iss.gitcloneteam.weatherobserver.model.Measurement;
import edu.iss.gitcloneteam.weatherobserver.services.MeasurementService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/measurement")
public class MeasurementForUserController {

    private MeasurementService measurementService;

    @Autowired
    public MeasurementForUserController(MeasurementService measurementService) {
        this.measurementService = measurementService;
    }

    @GetMapping("/current")
    public Measurement getCurrentMeasurement() {
        Measurement currentMeasurement = measurementService.getLastMeasurement();
        return currentMeasurement;
    }

    @GetMapping("/interval")
    public List<Measurement> getMeasurementsForTimeInterval(@RequestParam("duration") int durationInHours) {
        List<Measurement> measurementsForTimeInterval = measurementService.getMeasurementsForTimeInterval(durationInHours);
        return measurementsForTimeInterval;
    }

    @GetMapping("/average")
    public Measurement getAverageMeasurementForLast24Hours() {
        Measurement measurement = measurementService.getAverageMeasurementForLast24Hours();
        return measurement;
    }

}