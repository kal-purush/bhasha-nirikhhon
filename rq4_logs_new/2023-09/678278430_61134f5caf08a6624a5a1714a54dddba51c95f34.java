package com.openlabs.shoppingmall.test.controller;

import com.openlabs.shoppingmall.entity.Users;
import com.openlabs.shoppingmall.test.service.TestService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/admin/v1/test")
@Api(value = "어드민 테스트")
public class TestController {
    @Autowired
    TestService service;

    /** 서비스용 유저조회 서비스 */
    @GetMapping("/r-user")
    @ApiOperation(value = "유저조회 테스트")
    public List<Users> searchUsers(){

        return service.searchUsers();
    }
}