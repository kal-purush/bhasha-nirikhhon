package com.openlabs.shoppingmall.controller;

import com.openlabs.framework.dto.ResponseDto;
import com.openlabs.framework.exception.ShopException;
import com.openlabs.shoppingmall.dto.OrderItemTestResDto;
import com.openlabs.shoppingmall.dto.UserTestResDto;
import com.openlabs.shoppingmall.entity.OrderItem;
import com.openlabs.shoppingmall.entity.Users;
import com.openlabs.shoppingmall.service.TestService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/labshop/v1/test")
@Api(value = "어드민 테스트")
public class TestController {
    @Autowired
    TestService service;

    /** 서비스용 유저조회 서비스 */
    @GetMapping("/r-user")
    @ApiOperation(value = "유저전체조회 테스트")
    public ResponseDto<List<Users>> searchUsers(){
        try {
            return ResponseDto.ok(service.searchUsers());
        } catch (ShopException e) {
            return ResponseDto.error(401, "실패", null, null);
        }
    }

    /** 서비스용 유저조회 서비스 */
    @GetMapping("/r-user/detail")
    @ApiOperation(value = "유저단건조회 테스트")
    public ResponseDto<UserTestResDto> searchOneUsers(String userId){
        try {
            return ResponseDto.ok(service.searchOneUsers(userId));
        } catch (ShopException e) {
            return ResponseDto.error(401, "실패", null, null);
        }
    }

    /** 서비스용 유저생성 서비스 */
    @PostMapping("/c-user")
    @ApiOperation(value = "유저생성 테스트")
    public ResponseDto<UserTestResDto> createUsers(UserTestResDto user){
        try {
            return ResponseDto.ok(service.createUsers(user));
        } catch (ShopException e) {
            return ResponseDto.error(401, "실패", null, null);
        }
    }

    /** 서비스용 주문상품 단건조회 서비스 */
    @GetMapping("/r-orderItem/detail")
    @ApiOperation(value = "주문상품 단건조회 테스트")
    public ResponseDto<OrderItemTestResDto> searchOneOrderItem(Long orderItemId){
        try {
            return ResponseDto.ok(service.searchOneOrderItem(orderItemId));
        } catch (ShopException e) {
            return ResponseDto.error(401, "실패", null, null);
        }
    }
}