package com.BackendChallenge.TechTrendEmporium.controller;

import com.BackendChallenge.TechTrendEmporium.entity.User;
import com.BackendChallenge.TechTrendEmporium.entity.mapper.UserDto;
import com.BackendChallenge.TechTrendEmporium.entity.mapper.UserMapper;
import com.BackendChallenge.TechTrendEmporium.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;

@RestController
@RequestMapping("/api")
public class AuthController {

    @Autowired
    private UserService userService;
    @Autowired
    private UserMapper userMapper;

    @PostMapping("/auth")
    public ResponseEntity<UserDto> registerShopper(@RequestBody User user) {
        // Register shopper
        User registeredShopper = userService.registerShopper(user);
        UserDto registeredShopperDto = userMapper.toDto(registeredShopper);
        return new ResponseEntity<>(registeredShopperDto, HttpStatus.CREATED);
    }


}