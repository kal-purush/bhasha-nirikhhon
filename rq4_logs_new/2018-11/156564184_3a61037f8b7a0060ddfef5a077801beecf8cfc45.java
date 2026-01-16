package com.example.rseservice.controller;

import com.example.rseservice.domain.ServiceD;
import com.example.rseservice.domain.request.ServiceRequest;
import com.example.rseservice.domain.response.ServiceResponse;
import com.example.rseservice.service.implement.ServiceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;

@Controller
@RequestMapping(value="service")
public class ServiceController {

    @Autowired
    private ServiceService serviceS;

    @PostMapping()
    public ResponseEntity create(@Valid @RequestBody ServiceRequest serviceRequest) {
        ServiceD serviceResponse = serviceS.create(serviceRequest);
        return new ResponseEntity<>(serviceRequest, HttpStatus.CREATED);
    }

    @GetMapping(value = "/{serviceId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity getService(@PathVariable("serviceId") Long serviceId) {
        ServiceResponse service = serviceS.findById(serviceId);
        return new ResponseEntity<>(service, HttpStatus.OK);
    }

    @GetMapping(value = "/services/{clientId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity getServices(@PathVariable("clientId") Long clientId) {
        List<ServiceResponse> services = serviceS.findAll(clientId);
        return new ResponseEntity<>(services, HttpStatus.OK);
    }
}