package com.pet.pet_funeral.api;

import com.pet.pet_funeral.domain.user.service.GoogleService;
import com.pet.pet_funeral.security.dto.LoginResponse;
import com.pet.pet_funeral.utils.SuccessResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/v1/api/google")
public class GoogleController {
    // 로그인 창 호출 : https://accounts.google.com/o/oauth2/v2/auth
    // 구글 엑세스 토큰 요청 url : https://oauth2.googleapis.com/token
    private final GoogleService googleService;

    @GetMapping("/login")
    public ResponseEntity<?> login(@RequestParam String code){
        LoginResponse loginResponse = googleService.login(code);
        SuccessResponse response = new SuccessResponse(true,"구글 로그인 성공", loginResponse);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
//https://kauth.kakao.com/oauth/authorize?response_type=code&client_id=3c78ccc2c22f2c064f9e28457bcdb0c9&redirect_uri=http://localhost:8080/v1/api/kakao/login
    // 로그인 경로 호출 : https://accounts.google.com/o/oauth2/v2/auth?client_id=374636879296-tme6rr54rdpjua74vvnf8hpo7jjul3f4.apps.googleusercontent.com&redirect_uri=http://localhost:8080/v1/api/google/login&response_type=code&scope=openid%20email%20profile

}