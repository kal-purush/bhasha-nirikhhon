package com.drawnblue.user.controller;

import com.drawnblue.entity.User;
import com.drawnblue.user.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/user-center")
public class UserCenterController {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private UserService userService;

    @GetMapping("/findById/{id}")
    public User findById(@PathVariable Long id) {
        User user = this.userService.selectById(id);
/*        User user2 = new User();
        user2.setMobile("18801166946");
        user2.setPassword("123456");
        User usser2 = this.userService.findByMobileOrEmail(user2);*/
        return user;
    }
    @PostMapping("/findByMobileOrEmail")
    public User findByMobileOrEmail(@RequestBody User user){
        logger.info("userCenter --> findByMobileOrEmail : {}",user.toString());
        User usser = this.userService.findByMobileOrEmail(user);
        return user;
    }
}