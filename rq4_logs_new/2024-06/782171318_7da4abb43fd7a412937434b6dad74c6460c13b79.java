package com.artfriendly.artfriendly.domain.userlog.controller;

import com.artfriendly.artfriendly.domain.userlog.service.UserLogService;
import com.artfriendly.artfriendly.global.api.RspTemplate;
import lombok.RequiredArgsConstructor;

import org.springframework.http.HttpStatus;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("userlogs")
@RequiredArgsConstructor
public class UserLogController {
    private final UserLogService userLogService;

    @PostMapping("/location")
    public RspTemplate<Void> createLocationInfoLog(@AuthenticationPrincipal long memberId) {
        userLogService.createLocationInfoLog(memberId);

        return new RspTemplate<>(HttpStatus.OK, "위치 정보 로그 저장 완료");
    }
}