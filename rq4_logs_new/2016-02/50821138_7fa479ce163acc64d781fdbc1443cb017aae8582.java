package com.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.github.pagehelper.Page;
import com.model.rights.User;
import com.service.rights.UserService;
import com.system.page.PageParam;

/**
 * @Description:
 * @author zhaozhineng
 * @date 2016-2-15 下午2:37:50
 */
@Controller
@RequestMapping(value = "/bootstrap")
public class BootstrapController {

    @Autowired
    private UserService userService;

    @RequestMapping(value = "")
    public String test(Map<String, Object> model) {
        User param = new User();
        PageParam page = new PageParam(1, 10, "name desc");
        Page<User> userList = userService.queryByPage(param, page);
        model.put("userList", userList);
        return "bootstrap/test";
    }
}