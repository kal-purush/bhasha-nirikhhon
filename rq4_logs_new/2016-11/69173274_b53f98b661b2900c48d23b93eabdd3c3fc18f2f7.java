package com.thom.hystrix.controller;

import com.thom.hystrix.service.WechatMenuService;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.io.IOException;

@Controller
@RequestMapping("menu")
public class WechatMenuController {

    @Resource
    private WechatMenuService wechatMenuService;

    @RequestMapping(value = {"create"}, method = RequestMethod.GET)
    @ResponseBody
    public boolean create() throws IOException {
        return wechatMenuService.create();
    }
}