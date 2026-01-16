package com.example.map.controller;
import com.example.map.entity.UserEntity;
import com.example.map.repository.UserRepository;
import com.example.oauthjwt.entity.UserEntity;
import com.example.oauthjwt.repository.UserRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.Map;


@RestController
@RequestMapping("/api")
@Slf4j
public class AuthController {

    private final UserRepository userRepository;

    public AuthController(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @PostMapping("/check-account")
    public ResponseEntity<?> checkAccount(@RequestBody Map<String, String> request) {
        log.info("Check account request: {}", request);
        String account = request.get("account");
        System.out.println(account);

        // 이메일로 사용자 정보 조회
        UserEntity user = userRepository.findByEmail(account);
        System.out.println(user);

        if (user != null) {
            // 소셜 로그인 제공자 정보 반환 (username에서 제공자 정보 추출)
            String[] providerInfo = user.getUsername().split(" ");
            String provider = providerInfo[0]; // 소셜 제공자 (google, naver, kakao 등)
            log.info(provider);

            return ResponseEntity.ok(Collections.singletonMap("provider", provider));
        } else {
            // 등록되지 않은 계정일 경우
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("등록된 소셜 로그인 제공자가 없습니다.");
        }
    }
}
