package com.pet.pet_funeral.api;

import com.pet.pet_funeral.security.dto.LoginResponse;
import com.pet.pet_funeral.security.service.LoginService;
import com.pet.pet_funeral.utils.SuccessResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/v1/api/auth")
public class AuthController {

    private LoginService loginService;

    private ResponseEntity<?> createLoginResponse(UUID id){
        LoginResponse loginResponse = loginService.login(id);
        return ResponseEntity.ok()
                .header(HttpHeaders.AUTHORIZATION,loginResponse.getAccessToken())
                .header(HttpHeaders.SET_COOKIE,loginResponse.getRefreshTokenCookie().toString())
                .body(new SuccessResponse(true,"로그인 성공",loginResponse));

    }
}