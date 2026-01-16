package com.study.controller;

import com.study.common.BaseController;
import com.study.common.BaseRsp;
import com.study.common.IService;
import com.study.message.car.req.AddCarReq;
import com.study.message.role.req.CreateAccountReq;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@RequestMapping("/api")
@Controller
public class RoleController extends BaseController {

    @Autowired
    private IService<CreateAccountReq, BaseRsp> createAccountService;

    @RequestMapping(value = "/createAccount", method = RequestMethod.POST)
    @ResponseBody
    public BaseRsp addCar(@RequestBody CreateAccountReq req) {
        return super.service(createAccountService,req);
    }

}