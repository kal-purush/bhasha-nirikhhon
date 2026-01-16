package com.github.tomaszgryczka.pipelinecd;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BasicController {

    private final BasicService basicService;

    @Value("${app.version}")
    private String appVersion;

    public BasicController(BasicService basicService) {
        this.basicService = basicService;
    }

    @RequestMapping("/")
    String sayHello() {
        return basicService.returnHelloWorld() + "! App version: " + appVersion;
    }
}