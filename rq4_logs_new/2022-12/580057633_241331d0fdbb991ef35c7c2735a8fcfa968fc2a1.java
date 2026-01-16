package com.vitweb.vitwebapi.adapter.web.v1.controller;

import com.vitweb.vitwebapi.adapter.web.base.RestApiV1;
import com.vitweb.vitwebapi.adapter.web.base.VsResponseUtil;
import com.vitweb.vitwebapi.application.constants.UrlConstant;
import com.vitweb.vitwebapi.application.inputs.user.ChangeAvatarInput;
import com.vitweb.vitwebapi.application.inputs.user.UpdateUserInput;
import com.vitweb.vitwebapi.application.services.IUserService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestApiV1
public class UserController {

  private final IUserService userService;

  public UserController(IUserService userService) {
    this.userService = userService;
  }

  @GetMapping(UrlConstant.User.LIST)
  public ResponseEntity<?> getAll() {
    return VsResponseUtil.ok(userService.getAll());
  }

  @GetMapping(UrlConstant.User.GET)
  public ResponseEntity<?> getUserById(@PathVariable("id") String id) {
    return VsResponseUtil.ok(userService.getUserById(id));
  }

  @PatchMapping(UrlConstant.User.UPDATE)
  public ResponseEntity<?> updateUser(@ModelAttribute UpdateUserInput updateUserInput) {
    return VsResponseUtil.ok(userService.updateUser(updateUserInput));
  }

  @PostMapping(UrlConstant.User.CHANGE_AVATAR)
  public ResponseEntity<?> changeAvatarUser(@ModelAttribute ChangeAvatarInput changeAvatarInput) {
    return VsResponseUtil.ok(userService.changeAvatar(changeAvatarInput));
  }

  @DeleteMapping(UrlConstant.User.DELETE)
  public ResponseEntity<?> deleteUser(@PathVariable("id") String id) {
    return VsResponseUtil.ok(userService.deleteById(id));
  }
  
}