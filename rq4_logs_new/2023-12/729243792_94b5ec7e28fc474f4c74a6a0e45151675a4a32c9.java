package com.chwipoClova.tokenTest.controller;

import com.chwipoClova.user.response.UserInfoRes;
import com.chwipoClova.user.service.UserService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
@Tag(name = "TokenTest", description = "토큰 테스트 API")
@RequestMapping("test")
public class TokenTestController {
    private final UserService userService;


    @Operation(summary = "토큰 테스트 용", description = "유저 정보 조회 (테스트용)")
    @GetMapping("/getUserInfo")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "OK")
    }
    )
    public UserInfoRes getUserInfo(
            @Schema(description = "유저 ID", example = "1", name = "userId") @RequestParam(name = "userId") Long userId
    ) {
        return userService.selectUserInfoForUserId(userId);
    }
}