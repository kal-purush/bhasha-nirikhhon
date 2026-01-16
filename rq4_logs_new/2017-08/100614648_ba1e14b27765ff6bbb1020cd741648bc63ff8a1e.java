package com.jd.controller;

import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;


import java.util.Date;

/**
 * test index controller
 *
 * @author hasee
 * 2017-08-17
 */

@Controller
@RequestMapping(value = "/")
public class IndexController {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(IndexController.class);

    @RequestMapping(value = "/index")
    public ModelAndView IndexController(){
        logger.debug("step into method IndexController");
        ModelAndView mav = new ModelAndView();
        mav.addObject("msg","Hello World");
        mav.getModelMap().put("time",new Date());
        logger.debug("step out method IndexController");
        logger.info("execute method over");
        return mav;
    }

}