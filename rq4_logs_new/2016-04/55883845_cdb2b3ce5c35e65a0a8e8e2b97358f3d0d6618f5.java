package com.example.controller.test;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * Created by arahansa on 2016-04-10.
 */
@Slf4j
@RestController
@RequestMapping("/test")
public class TestProfileController {

    @Resource
    private Environment env;

    @RequestMapping("/profile")
    public String test(){
        return env.getRequiredProperty("test.msg");
    }


}