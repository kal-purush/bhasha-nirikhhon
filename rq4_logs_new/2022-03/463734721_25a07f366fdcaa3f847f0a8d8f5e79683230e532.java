package com.springmvc.controller;

import com.springmvc.entity.User;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

/**
 * @author: lijinhao
 * @date: 2022/03/01 17:43
 */

@Controller
//@RequestMapping("/test")
public class URLMappingController {
    @GetMapping("/g")
    @ResponseBody
    public String getMapping(@RequestParam("manager_name") String managerName){
        System.out.println("manager:" + managerName);
        return "This is a get method";
    }

    @GetMapping("/view")
    public ModelAndView showView(Integer userId){
        ModelAndView mav = new ModelAndView("/view.jsp");
        User user = new User();
        if(userId == 1){
            user.setUsername("lily");
        }else if(userId == 2){
            user.setUsername("smith");
        }else if(userId == 3){
            user.setUsername("lina");
        }
        mav.addObject("u", user);
        return mav;
    }
}