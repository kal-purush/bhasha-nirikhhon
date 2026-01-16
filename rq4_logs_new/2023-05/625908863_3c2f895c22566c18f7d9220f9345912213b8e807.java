package com.brainstrom.meokjang.controller;

import com.brainstrom.meokjang.domain.User;
import com.brainstrom.meokjang.dto.UserDto;
import com.brainstrom.meokjang.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@Controller
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    @PostMapping("/users/create")
    public String create(@RequestBody UserDto dto, BindingResult result) {

        if (result.hasErrors()) {
            throw new IllegalStateException("잘못된 입력입니다.");
        }

        User user = new User();
        user.setUserName(dto.getUserName());
        user.setPhoneNumber(dto.getPhoneNumber());
        user.setSnsType(dto.getSnsType());
        user.setSnsKey(dto.getSnsKey());
        user.setLocation(dto.getLocation());
        user.setLatitude(dto.getLatitude());
        user.setLongitude(dto.getLongitude());
        user.setGender(dto.getGender());
        user.setReliability((float)50);

        userService.join(user);
        return "redirect:/";
    }

    @PostMapping("/users/login")
    public String login(@RequestBody UserDto dto, BindingResult result) {

        if (result.hasErrors()) {
            throw new IllegalStateException("잘못된 입력입니다.");
        }

        userService.login(dto.getPhoneNumber(), dto.getSnsType(), dto.getSnsKey());
        return "redirect:/";
    }
}