package kcl.paramount.group.controller;

import kcl.paramount.group.business.UserBusiness;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ForgetPasswordController {
    @RequestMapping(value = "/forget", method = RequestMethod.GET)
    public String forgetPassword(@RequestParam("username") String username,
                                 @RequestParam("answer1") String answer1,
                                 @RequestParam("answer2") String answer2,
                                 @RequestParam("password") String password) {
        String result = null;
        UserBusiness ub = new UserBusiness();
        result = ub.forgetPassword(username, answer1, answer2, password);
        return result;
    }
}