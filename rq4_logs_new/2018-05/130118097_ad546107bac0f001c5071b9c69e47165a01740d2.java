package me.zonglun.seckilling.controller;

import me.zonglun.seckilling.domain.Result;
import me.zonglun.seckilling.domain.SeckillUser;
import me.zonglun.seckilling.redis.RedisService;
import me.zonglun.seckilling.service.SeckillUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/user")
public class UserController {

	@Autowired
    SeckillUserService userService;
	
	@Autowired
    RedisService redisService;
	
    @RequestMapping("/info")
    @ResponseBody
    public Result<SeckillUser> info(Model model, SeckillUser user) {
        return Result.success(user);
    }
    
}