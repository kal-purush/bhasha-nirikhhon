package com.luximed.client.appointment;

import com.luximed.client.dto.AppointmentDto;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.List;

@FeignClient(name = "database-service")
public interface ClientDatabase {

    @GetMapping(value = "/api/database/appointment/all")
    List<AppointmentDto> getAppointments();

    @GetMapping(value = "/api/database/appointment/{id}")
    AppointmentDto getAppointmentById(@PathVariable Integer id);

    @GetMapping(value = "/api/database/appointment/patient/{id}")
    List<AppointmentDto> getAppointmentsByPatientId(@PathVariable Integer id);
}