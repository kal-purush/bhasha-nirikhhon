package com.example.docconneting.domain.alarm.service;

import com.example.docconneting.common.exception.constant.ErrorCode;
import com.example.docconneting.common.exception.object.ClientException;
import com.example.docconneting.domain.user.entity.User;
import com.example.docconneting.domain.user.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class FcmTokenService {

    private final UserRepository userRepository;

    /*
     * 유효하지 않은 FCM 토큰을 가진 사용자의 FCM 토큰을 제거
     */
    @Transactional
    public void deleteFcmToken(String fcmToken) {
        User user = userRepository.findByFcmToken(fcmToken).orElseThrow(() -> new ClientException(ErrorCode.USER_NOT_FOUND));
        user.deleteFcmToken();
    }

}