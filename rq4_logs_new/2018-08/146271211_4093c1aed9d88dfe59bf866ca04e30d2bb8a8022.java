package com.team6.controller;

import com.team6.entity.User;
import com.team6.service.login.LoginService;
import com.team6.util.jwtUtil;
import org.apache.ibatis.annotations.Param;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;

@Controller
public class LoginController {
    @Autowired
    private LoginService loginService;
    @RequestMapping(value = "/login",method = RequestMethod.POST )
    public String login(@Param("name") String name, @Param("password") String password, HttpServletResponse response){
        String token = loginService.Login(name,password);
        Cookie cookie = new Cookie("token",token);
        cookie.setPath("/");
        response.addCookie(cookie);
        if(token==null) return "loginError";
        else
        return "loginSuccess";
    }
    @RequestMapping(value = "/regiest",method = RequestMethod.POST)
    public String regiest(User user,HttpServletResponse response){
        System.out.println("user:"+user.toString());
      String state = loginService.regiest(user);
      if(state.equals("SUCCESS")){
          String token = jwtUtil.createToken(user);
          Cookie cookie = new Cookie("token",token);
          cookie.setPath("/");
          response.addCookie(cookie);
          return "regiestSuccess";
      }else
          return "regiestError";
    }
}