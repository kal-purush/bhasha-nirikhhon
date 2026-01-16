package com.example.hackathon.user.application;

import com.example.hackathon.exception.BadRequestException;
import com.example.hackathon.user.domain.User;
import com.example.hackathon.user.dto.UserInfoResponse;
import com.example.hackathon.user.repository.UserRepository;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
@AllArgsConstructor
@Service
public class MypageService {
    private final UserRepository userRepository;
    public UserInfoResponse getUserInfo(Long userId){
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new BadRequestException("유저 토큰 값을 다시 확인해주세요"));
        UserInfoResponse userInfoResponse = UserInfoResponse.builder()
                .email(user.getEmail())
                .profileImage(user.getImageUrl())
                .username(user.getName())
                .build();
        return userInfoResponse;
    }

    public String updateUserName(Long userId, String nickName){
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new BadRequestException("유저 토큰 값을 다시 확인해주세요"));
        user.setName(nickName);
        userRepository.save(user);
        return "success update nickname";
    }

}