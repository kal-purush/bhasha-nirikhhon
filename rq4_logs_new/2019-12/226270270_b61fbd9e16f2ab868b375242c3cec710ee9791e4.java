package com.gxl.framework.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.Serializable;

/**
 * @author weilai
 * @description
 * @since 2019/12/18
 */
@Controller
@RequestMapping("/demo")
public class DemoController implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(DemoController.class);

    @RequestMapping("/test")
    public String test(){
        return "hello world!!!";
    }
}