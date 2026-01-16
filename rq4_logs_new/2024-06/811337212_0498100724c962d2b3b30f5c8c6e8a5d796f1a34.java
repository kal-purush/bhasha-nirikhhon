package com.example.psychology_back.controller;

import com.example.psychology_back.dto.UserSettingDto;
import com.example.psychology_back.service.UserSettingService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin(value = "http://localhost:3000")
@RequiredArgsConstructor
public class UserSettingController {

    private final UserSettingService userSettingService;

    @GetMapping("/api/v1/usertheme")
    public UserSettingDto getTests() {
        return userSettingService.getUserSetting();
    }

    @PostMapping("/api/v1/usertheme")
    public UserSettingDto setTests(@RequestBody UserSettingDto userSettingDto) {
        return userSettingService.setUserSetting(userSettingDto);
    }

}