package com.zhy.controller;

import com.zhy.bean.User;
import org.springframework.stereotype.Controller;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.validation.Valid;

/**
 * @ClassName: DataValidateController
 * @Description: TODO 留下注释吧
 * @Date: 2020/5/14 15:29
 * @Version: 1.0
 **/
@Controller
public class DataValidateController {

    @RequestMapping("/dataValidate")
    public String validate(@Valid User user, BindingResult bindingResult) {
        System.out.println(user);
        if (bindingResult.hasErrors()) {
            System.out.println("验证失败");
            return "redirect:/index.jsp";
        } else {
            System.out.println("验证成功");
            return "success";
        }
    }
}