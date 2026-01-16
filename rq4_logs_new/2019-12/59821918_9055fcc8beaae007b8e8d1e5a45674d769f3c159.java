package org.cplier.codegen.controller;

import lombok.extern.slf4j.Slf4j;
import org.cplier.codegen.entity.User;
import org.cplier.codegen.service.UserService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

import javax.annotation.Resource;
import javax.validation.Valid;

@Slf4j
@Controller
public class UserController {

  @Resource UserService userService;

  @GetMapping("/signup")
  public String showSignUpForm(User user, Model model) {
    model.addAttribute("user", user);
    return "add-user";
  }

  @PostMapping("/adduser")
  public String addUser(@Valid User user, BindingResult result, Model model) {
    if (result.hasErrors()) {
      return "add-user";
    }

    User update = userService.save(user);
    log.debug("the user={} has been created", update.getId());
    model.addAttribute("users", userService.findAll());
    return "index";
  }

  @PostMapping("/update/{id}")
  public String updateUser(
      @PathVariable("id") long id, @Valid User user, BindingResult result, Model model) {
    if (result.hasErrors()) {
      user.setId(id);
      return "update-user";
    }

    userService.save(user);
    model.addAttribute("users", userService.findAll());
    return "index";
  }

  @GetMapping("/delete/{id}")
  public String deleteUser(@PathVariable("id") long id, Model model) {
    User user =
        userService
            .findById(id)
            .orElseThrow(() -> new IllegalArgumentException("Invalid user Id:" + id));
    userService.delete(user);
    model.addAttribute("users", userService.findAll());
    return "index";
  }
}