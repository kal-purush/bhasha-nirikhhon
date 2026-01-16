package com.project.medicumzone.ui.controller;

import com.project.medicumzone.io.enitity.Clinic;
import com.project.medicumzone.service.ClinicService;
import com.project.medicumzone.ui.model.request.NewClinicRequestModel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;

@RestController
@Slf4j
@AllArgsConstructor
@RequestMapping("/api/clinics")
public class ClinicController {

    @Autowired
    private final ClinicService clinicService;

    @GetMapping
    public List<Clinic> getAllClinics(){
        return clinicService.getAllClinics();
    }

    @PostMapping("/add")
    public void addNewClinic(@Valid @RequestBody NewClinicRequestModel clinicToAdd){
        clinicService.addNewClinic(clinicToAdd);
    }
}