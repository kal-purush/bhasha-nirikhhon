package com.chapssal_tteok.preview.domain.user.controller;

import com.chapssal_tteok.preview.domain.user.dto.*;
import com.chapssal_tteok.preview.domain.user.entity.Role;
import com.chapssal_tteok.preview.domain.user.service.UserAuthService;
import com.chapssal_tteok.preview.domain.user.service.UserCommandService;
import com.chapssal_tteok.preview.global.apiPayload.ApiResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
@Slf4j
public class UserAuthController {

    private final UserCommandService userCommandService;
    private final UserAuthService userAuthService;

    @PostMapping("/register")
    public ApiResponse<RegisterResponseDTO> register (@RequestBody @Valid UserRequestDTO.CreateUserDTO userRequestDto){
        RegisterResponseDTO responseDto = userCommandService.registerUser(userRequestDto);

        return ApiResponse.onSuccess(responseDto);
    }

    @PostMapping("/login")
    public ApiResponse<JwtResponse> login (@RequestBody @Valid LoginRequestDTO loginRequestDto) {
        JwtResponse jwtResponse = userAuthService.login(loginRequestDto);

        return ApiResponse.onSuccess(jwtResponse);
    }


    @PostMapping("/refresh")
    public ApiResponse<JwtResponse> refresh(@RequestBody RefreshRequestDTO refreshRequestDTO) {
        String refreshToken = refreshRequestDTO.getRefreshToken();
        log.info("🔁 Refresh Token: {}", refreshToken);

        JwtResponse jwtResponse = userAuthService.refresh(refreshToken);

        log.info("🔁 Refreshed Token: new access token: {}", jwtResponse.getAccessToken());
        return ApiResponse.onSuccess(jwtResponse);
    }

    @PatchMapping("/{user_id}/role")
    @PreAuthorize("hasRole('ADMIN')")
    public ApiResponse<UserResponseDTO.UserInfoDTO> changeRole(@PathVariable Long user_id, @RequestParam Role newRole) {
        UserResponseDTO.UserInfoDTO userInfoDTO = userAuthService.updateUserRole(user_id, newRole);

        return ApiResponse.onSuccess(userInfoDTO);
    }
}