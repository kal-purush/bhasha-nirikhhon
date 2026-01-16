package com.iwuyc.springboot.web.controllers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

@RequestMapping("main")
@Controller
public class MainController {

    private static final Logger LOGGER = LoggerFactory.getLogger(MainController.class);

    @RequestMapping("index")
    public String index(@RequestParam String page){
        LOGGER.info(page);
        return page;
    }

    @RequestMapping("goto")
    public ModelAndView gotoPage(@RequestParam String page){
        LOGGER.info(page);
        ModelAndView view =new ModelAndView();
        view.setViewName(page);
        return view;
    }
}