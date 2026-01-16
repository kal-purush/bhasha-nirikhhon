package com.project.sidefit.api.controller;

import com.project.sidefit.api.dto.response.Response;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/sidefit")
@RequiredArgsConstructor
public class HealthcheckController {

    @GetMapping("/healthcheck")
    public Response healthcheck() {

        return Response.success("service is health");
    }

}