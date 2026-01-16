package com.yanghang.feignclient.controller;

import com.yanghang.feignclient.feignservice.UserFeignService;
import com.yanghang.feignclient.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @program: springcloudcode
 * @description: 测试用户类
 * @author: YangHang
 * @create: 2019-08-12 22:22
 **/

@RestController
@RequestMapping("/user")
public class UserController {

    @Autowired
    private UserFeignService userFeignService;

    @RequestMapping("/addUser")
    public String addUser() {
        return userFeignService.addUser("YH", 25);
    }

    @RequestMapping("/updateUser")
    public String updateUser() {
        User yh = User.builder().age(25).name("YH").build();
        return userFeignService.updateUser(yh);
    }
}