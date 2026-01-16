package com.example.rseservice.controller;

import com.example.rseservice.domain.request.ServiceHistoriesRequest;
import com.example.rseservice.domain.response.ServiceHistoriesResponse;
import com.example.rseservice.service.implement.ServiceHistoriesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;

@Controller
@RequestMapping(value="serviceHistories")
public class ServiceHistoriesController {

    @Autowired
    private ServiceHistoriesService serviceHistoriesService;

    @PostMapping()
    public ResponseEntity create(@Valid @RequestBody ServiceHistoriesRequest serviceHistoriesRequest) {
        ServiceHistoriesResponse serviceResponse = serviceHistoriesService.create(serviceHistoriesRequest);
        return new ResponseEntity<>(serviceResponse, HttpStatus.CREATED);
    }

    @GetMapping(value = "/{serviceHistories}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity getServiceHistories(@PathVariable("serviceHistories") Long serviceHistories) {
        ServiceHistoriesResponse serviceHistoriesResponse = serviceHistoriesService.findById(serviceHistories);
        return new ResponseEntity<>(serviceHistoriesResponse, HttpStatus.OK);
    }

    @GetMapping(value = "/getServices/{serviceHistories}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity getServices(@PathVariable("serviceHistories") Long serviceHistories) {
        List<ServiceHistoriesResponse> services = serviceHistoriesService.findAllServiceId(serviceHistories);
        return new ResponseEntity<>(services, HttpStatus.OK);
    }
}