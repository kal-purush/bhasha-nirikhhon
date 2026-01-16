package com.office.system.ssm.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletResponse;

/**
 * @author Chen
 * @createTime 2019106 23:01
 * @description the user controller for test
 */
@Controller
public class UserController {
    @RequestMapping(value = "/index")
    public ModelAndView index(Model model, HttpServletResponse response) {
        ModelAndView modelAndView = new ModelAndView("index.jsp");
        return modelAndView;
    }
}