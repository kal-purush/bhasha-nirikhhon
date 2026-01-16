/*
 * Non-RESTful，关于News的HTTP请求由这个类Handle
 * @author: Jiazhen Zhang
 * @version:0.0.1, 2018/05/04
 * */
package com.spitfire.forexmemo.web;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class WebNewsPieceCtrler {

    @RequestMapping(value = "/postnews", method = RequestMethod.GET)
    public String postNews(Model model) {
        return "postnews";
    }

}