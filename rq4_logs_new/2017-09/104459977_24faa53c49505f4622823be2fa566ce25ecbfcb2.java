package org.macula.cart.webapp.controller;

import org.macula.core.controller.BaseController;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Created by donh on 2017/9/26.
 */
@Controller
@RequestMapping({"/test"})
public class TestController extends BaseController {

    @RequestMapping(value = {"/hello"}, method = {RequestMethod.GET})
    public String hello(HttpServletRequest request, HttpServletResponse response) {
        System.out.println("-----------------进入测试方法-----------");
        return "index";

    }
}