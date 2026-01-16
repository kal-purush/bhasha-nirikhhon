package com.example.board.controller;

import static com.example.board.domain.type.UserType.*;

import com.example.board.domain.dto.UserDto;
import com.example.board.service.SignUpService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequiredArgsConstructor
public class SignUpController {

    private final SignUpService signUpService;


    // 관리자 회원가입
    @PostMapping("/signup/admin")
    public ResponseEntity<String> adminSignUp(@RequestBody UserDto.SignUp signUp) {
        return ResponseEntity.ok(signUpService.signUp(signUp, ADMIN));
    }


    // 회원 회원가입
    @PostMapping("/signup/user")
    public ResponseEntity<String> userSignUp(@RequestBody UserDto.SignUp signUp) {
        return ResponseEntity.ok(signUpService.signUp(signUp, USER));
    }
}