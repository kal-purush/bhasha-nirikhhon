package com.example.lifeonhana.controller;

import com.example.lifeonhana.ApiResult;
import com.example.lifeonhana.dto.response.UserResponseDTO;
import com.example.lifeonhana.service.UserService;
import com.example.lifeonhana.global.exception.UnauthorizedException;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
@Tag(name = "User", description = "사용자 관련 API")
public class UserController {
    private final UserService userService;

    @Operation(summary = "사용자 정보 조회", description = "현재 로그인한 사용자의 정보를 조회합니다.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "조회 성공"),
        @ApiResponse(responseCode = "401", description = "인증이 필요합니다."),
        @ApiResponse(responseCode = "404", description = "사용자를 찾을 수 없습니다.")
    })
    @GetMapping("/info")
    @SecurityRequirement(name = "bearerAuth")
    public ResponseEntity<ApiResult> getUserInfo(
        @Parameter(hidden = true)
        @AuthenticationPrincipal String authId
    ) {
        if (authId == null || authId.isEmpty()) {
            throw new UnauthorizedException("로그인이 필요한 서비스입니다.");
        }

        UserResponseDTO response = userService.getUserInfo(authId);
        
        return ResponseEntity.ok(ApiResult.builder()
            .code(HttpStatus.OK.value())
            .status(HttpStatus.OK)
            .message("사용자 정보 조회 성공")
            .data(response)
            .build());
    }
}