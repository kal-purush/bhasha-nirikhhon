package com.gandazhi.sell.controller;

import com.gandazhi.sell.common.ServiceResponse;
import com.gandazhi.sell.service.IUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;

/**
 * 后台有关用户的接口
 * @author gandazhi
 * @create 2017-12-07 上午9:48
 **/
@RestController
@RequestMapping("/api/seller/user")
public class ApiSellerUserController {

    @Autowired
    private IUserService iUserService;

    @PostMapping("/login")
    public ServiceResponse login(HttpServletResponse response, String username, String password){
        return iUserService.login(response, username, password);
    }
}