package com.evtimov.landlordapp.landlordspring.controllers;

import com.evtimov.landlordapp.landlordspring.models.Report;
import com.evtimov.landlordapp.landlordspring.services.ReportService;
import com.evtimov.landlordapp.landlordspring.services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.Optional;

@RestController
@RequestMapping(value = "/api/reports")
public class ReportController {
    private final ReportService service;

    @Autowired
    public ReportController(ReportService service) {
        this.service = service;
    }

    //Test method
    @RequestMapping(value = "/{reportId}", method = RequestMethod.GET)
    public Optional<Report> getMovieById(@PathVariable(value = "reportId") @Valid int id){
        return service.getReportById(id);
    }
}