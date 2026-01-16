package com.exadel.carinsurance.controller;

import com.exadel.carinsurance.entity.UserEntity;
import com.exadel.carinsurance.service.IUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
public class UserController {
  @Autowired
  private IUserService userService;

  @GetMapping("/users")
  public List<UserEntity> findAll() {
    return userService.findAll();
  }
}