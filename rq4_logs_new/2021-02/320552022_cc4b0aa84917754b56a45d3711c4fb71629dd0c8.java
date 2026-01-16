package com.netcracker.controller;

import com.netcracker.model.Status;
import com.netcracker.service.StatusService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatusController {
    private StatusService statusService;

    @Autowired
    public StatusController(StatusService statusService) {
        this.statusService = statusService;
    }

    @GetMapping("/status/id")
    public Status displayById(@RequestBody Status status) {
        return statusService.displayById(status);
    }
    @PostMapping ("/status/create")
    public Status createStatus (@RequestBody Status status){
        statusService.createStatus (status);
        return  status;
    }
}