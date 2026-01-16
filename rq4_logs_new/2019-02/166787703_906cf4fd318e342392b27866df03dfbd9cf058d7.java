package kcl.paramount.group.controller;

import com.alibaba.fastjson.JSONObject;
import kcl.paramount.group.business.UserBusiness;
import kcl.paramount.group.dao.UserDao;
import kcl.paramount.group.dao.UserDaoJDBCImpl;
import kcl.paramount.group.util.JSONUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SignUpController {
    @RequestMapping(value = "/signup", method = RequestMethod.GET)
    public String signUp(@RequestParam("username") String username, @RequestParam("password") String password,
                        @RequestParam("answer1") String answer1, @RequestParam("answer2") String answer2) {
        String result = null;
        UserBusiness ub = new UserBusiness();
        result = ub.addUser(username, password, answer1, answer2);
        return result;
    }
}