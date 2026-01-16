package com.vmax.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@RequestMapping("test")
@Controller
public class TestController extends BaseController {

    private static Logger logger = LoggerFactory.getLogger(TestController.class);

    @RequestMapping(value = "edit", method = RequestMethod.GET)
    public String testEdit(){
        logger.info("测试编辑工具");
        return "admin/test/edit" ;
    }
}
