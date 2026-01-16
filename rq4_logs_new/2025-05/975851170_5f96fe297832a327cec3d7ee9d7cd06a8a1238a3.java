package com.example.demo.service.impl;

import com.example.demo.service.UserService;
import com.example.demo.domain.User;
import com.example.demo.repository.RefreshTokenRepository;
import com.example.demo.repository.UserRepository;
import com.example.demo.dto.*;
import com.example.demo.exception.CustomException;
import com.example.demo.exception.ErrorCode;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;


@Service
@Transactional
@RequiredArgsConstructor
public class UserServiceImpl implements UserService {

    private final UserRepository            userRepository;
    private final RefreshTokenRepository    rfTokenRepo;
    private final BCryptPasswordEncoder     passwordEncoder;


    /**
     * 회원 가입
     * 
     * 회원가입 정보들을 저장하여 사용자를 회원가입 처리
     */
    @Override
    public void signup(SignupRequestDto signupRequestDto) {

        // 사용자 중복 검사(존재할 시 예외처리)
        validateDuplicateUsername(signupRequestDto.getUsername());

        // 사용자 원문 비밀번호를 BCrypt 해시 알고리즘으로 암호화
        String encodedPassword = passwordEncoder.encode(signupRequestDto.getPassword());

        // 회원가입 요청 정보를 바탕으로 User 엔티티 생성
        User newUser = User.builder()
            .username(signupRequestDto.getUsername())
            .password(encodedPassword)
            .name(signupRequestDto.getName())
            .phone(signupRequestDto.getPhone())
            .email(signupRequestDto.getEmail())
            .role("ROLE_USER") // Default
            .build();

        // 회원 정보 DB에 저장
        userRepository.save(newUser);
    }

    /** 
     * 회원 정보 조회
     * 
     * 사용자 정보를 username 기준으로 조회
    */
    @Override
    public UserInfoResponseDto getMyInfoByUsername(String username) {

        // 사용자 조회(존재하지 않으면 예외처리)
        User user = findUserOrThrow(username);

        // 응답용 DTO 반환
        return UserInfoResponseDto.from(user);
    }
    
    /**
     * 회원 정보 수정
     * 
     * 수정 가능한 사용자 정보(이름, 전화번호, 이메일)를 수정
     */
    @Override
    public UserInfoResponseDto updateUserInfo(String username, UpdateUserInfoDto updateUserInfoDto) {

        // 사용자 조회(존재하지 않으면 예외처리)
        User user = findUserOrThrow(username);

        // 전달받은 정보로 사용자 객체 필드 수정
        user.setName(updateUserInfoDto.getName());
        user.setPhone(updateUserInfoDto.getPhone());
        user.setEmail(updateUserInfoDto.getEmail());

        // 변경 사항 DB에 반영
        User saved = userRepository.save(user);

        // 응답용 DTO 반환
        return UserInfoResponseDto.from(saved);
    }

    /**
     * 비밀번호 변경
     * 
     * 사용자가 입력한 현재 비밀번호 일치 여부 검증 후 새 비밀번호로 변경
     */
    @Override
    public void changePassword(String username, ChangePasswordRequestDto changePasswordRequestDto) {

        // 사용자 조회(존재하지 않으면 예외처리)
        User user = findUserOrThrow(username);
        
        // 현재 비밀번호 검증(불일치 시 예외 처리)
        if(!passwordEncoder.matches(changePasswordRequestDto.getOldPassword(), user.getPassword())) {
            throw new CustomException(ErrorCode.INVALID_CREDENTIALS);
        }

        // 새 비밀번호 암호화 후 저장
        String encodedNew = passwordEncoder.encode(changePasswordRequestDto.getNewPassword());
        user.setPassword(encodedNew);

        // 변경 사항 DB에 반영
        userRepository.save(user);
    }

    /**
     * 회원 탈퇴
     * 
     * 해당 사용자의 모든 리프레시 토큰을 삭제하고,
     * 사용자 엔티티를 삭제
     */
    @Override
    public void withdraw(String username) {

        // 해당 사용자의 리프레시 토큰 전부 삭제
        rfTokenRepo.deleteAllByUsername(username);

        // 사용자 조회(존재하지 않으면 예외처리)
        User user = findUserOrThrow(username);

        // 사용자 엔티티 DB에서 삭제
        userRepository.delete(user);
    }


// =====================================================
// Helper Methods
// =====================================================

    /**
     * 사용자 조회 및 예외 처리
     * 
     * 주어진 username이 DB에 존재하는지 확인 후,
     * 존재하지 않으면 CustomException 발생시킴
     */
    private User findUserOrThrow(String username) {
        return userRepository.findByUsername(username)
            .orElseThrow(() -> new CustomException(ErrorCode.USER_NOT_FOUND));
    }

    /**
     * 사용자 중복 확인 및 예외 처리
     * 
     * 주어진 username이 이미 존재하는지 확인 후,
     * 존재할 시 CustomException 발생시킴
     */
    private void validateDuplicateUsername(String username) {
        if (userRepository.findByUsername(username).isPresent()) {
            throw new CustomException(ErrorCode.DUPLICATE_USERNAME);
        }
    }

}