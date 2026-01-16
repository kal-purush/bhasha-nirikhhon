package com.jiyongjun;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by jiyj on 2017/7/20.
 */
@RestController
@EnableAutoConfiguration
public class Example {
    @RequestMapping("/hello")
    String home(){

        return "Hello World!";
    }

    public static void main(String[] args) {
        SpringApplication.run(Example.class,args);
    }
}
