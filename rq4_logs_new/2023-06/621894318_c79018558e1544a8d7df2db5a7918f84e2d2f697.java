package com.gxdxx.instagram.service.user;

import com.gxdxx.instagram.dto.request.UserSignUpRequest;
import com.gxdxx.instagram.dto.response.UserSignUpResponse;
import com.gxdxx.instagram.entity.User;
import com.gxdxx.instagram.exception.NicknameAlreadyExistsException;
import com.gxdxx.instagram.repository.UserRepository;
import com.gxdxx.instagram.service.S3Uploader;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Transactional
@RequiredArgsConstructor
@Service
public class UserCreateService {

    private final UserRepository userRepository;
    private final S3Uploader s3Uploader;
    private final PasswordEncoder passwordEncoder;

    public UserSignUpResponse createUser(UserSignUpRequest request) {
        userRepository.findByNickname(request.nickname()).ifPresent(user -> {
            throw new NicknameAlreadyExistsException();
        });

        String profileImageUrl = s3Uploader.upload(request.profileImage(), "images");

        User saveUser = User.of(request.nickname(), passwordEncoder.encode(request.password()), profileImageUrl);
        return UserSignUpResponse.of(userRepository.save(saveUser));
    }

}