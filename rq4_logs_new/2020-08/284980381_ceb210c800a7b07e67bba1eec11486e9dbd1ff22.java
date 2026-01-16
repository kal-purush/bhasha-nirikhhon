package com.autohome.api.controller;

import com.autohome.api.service.LoginService;
import com.autohome.common.vo.R;
import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@Api(tags = "用户登录模块接口")
@RestController
@RequestMapping("/api/loginservice")
public class LoginController {
    @Autowired
    private LoginService loginService;
    @GetMapping("/userlogin.do/{str}/{psw}")
   public R checkUser(@PathVariable String str, @PathVariable String psw){
       return loginService.checkUser(str,psw);
    }
}