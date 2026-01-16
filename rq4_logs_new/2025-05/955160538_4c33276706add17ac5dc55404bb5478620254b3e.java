package com.iot7.controller;

import com.iot7.dto.PushTokenRequest;
import com.iot7.entity.User;
import com.iot7.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/push-token")
public class PushTokenController {

    private final UserRepository userRepository;

    // ✅ 푸시 토큰 저장 API
    @PostMapping
    public ResponseEntity<String> savePushToken(@RequestBody PushTokenRequest request) {
        Optional<User> optionalUser = userRepository.findById(request.getUserId());

        if (optionalUser.isPresent()) {
            User user = optionalUser.get();
            user.setPushToken(request.getPushToken());
            userRepository.save(user);

            return ResponseEntity.ok("푸시 토큰 저장 완료!");
        } else {
            return ResponseEntity.badRequest().body("존재하지 않는 사용자입니다.");
        }
    }
}