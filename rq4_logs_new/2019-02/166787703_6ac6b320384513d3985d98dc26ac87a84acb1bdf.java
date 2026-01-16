package kcl.paramount.group.controller;

import kcl.paramount.group.business.UserBusiness;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ChangePasswordController {
    @RequestMapping(value = "/changepassword", method = RequestMethod.GET)
    public String changePassword(@RequestParam("username") String username, @RequestParam("password") String password, @RequestParam("newpassword") String newpassword) {
        String result = null;
        UserBusiness ub = new UserBusiness();
        result = ub.changePassword(username, password, newpassword);
        return result;
    }
}