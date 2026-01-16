package com.luximed.reservation.controller;

import com.luximed.reservation.client.DatabaseClient;
import com.luximed.reservation.dto.AppointmentDto;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("api/appointment")
public class AppointmentController {
    private final DatabaseClient databaseClient;


    public AppointmentController(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    @PostMapping(value = "add")
    public void addAppointment(@RequestParam Integer patientId,
                              @RequestParam String date,
                              @RequestParam String dateTime,
                              @RequestParam Integer clinicId,
                              @RequestParam Integer doctorId,
                              @RequestParam Integer appointmentTypeId){
        databaseClient.addAppointment(patientId, date, dateTime, clinicId, doctorId, appointmentTypeId);
    }

    @GetMapping(value = "{id}")
    public AppointmentDto getAppointment(@PathVariable Integer id){
        return databaseClient.getAppointment(id);
    }

}