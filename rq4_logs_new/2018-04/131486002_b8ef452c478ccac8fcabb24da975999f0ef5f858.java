package com.springboot.class02;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@SpringBootApplication
@Configuration
public class HelloBootApplication {
    @RequestMapping("hello")
    @ResponseBody
    public String hello(){
        return "hello world!!!!";
    }

    public static void main(String[] args) {
        SpringApplication.run(HelloBootApplication.class,args);
    }

    //After Run this, you can connect to http://localhost:8080/hello  to get the return string.
    //@SpringBootApplication is spring boot's auto configuration order !!
    //

    /**
     *  Entry Point: XXXXApplication.class and main method
     *
     *  Combined Annotation:
     *  @SpringBootConfiguration : including: @Target,@Retention,@Documented, @Configuration
     *  Officially recommended to use @SpringBootConfigurations
     *
     *
     *
     */
}