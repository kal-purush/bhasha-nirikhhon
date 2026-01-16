package com.example.board.service.impl;

import static com.example.board.exception.ErrorCode.ALREADY_LOGIN_ID;
import static com.example.board.exception.ErrorCode.ALREADY_NICK_NAME;
import static com.example.board.exception.ErrorCode.BLANK_LOGIN_ID;
import static com.example.board.exception.ErrorCode.BLANK_NICK_NAME;
import static com.example.board.exception.ErrorCode.BLANK_PASSWORD;
import static com.example.board.exception.ErrorCode.NOT_FIND_LOGIN_ID;
import static com.example.board.exception.ErrorCode.WRONG_LOGIN_PASSWORD;

import com.example.board.domain.dto.UserDto;
import com.example.board.domain.entity.User;
import com.example.board.domain.repository.UserRepository;
import com.example.board.exception.GlobalException;
import com.example.board.security.TokenProvider;
import com.example.board.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


@Service
@RequiredArgsConstructor
public class UserServiceImpl implements UserService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final TokenProvider tokenProvider;


    // 회원가입
    @Transactional
    public String userSignUp(UserDto.SignUp signUp) {
        // 아이디 확인
        if (signUp.getLoginId().equals("")) {
            throw new GlobalException(BLANK_LOGIN_ID);
        } else if (userRepository.existsByLoginId(signUp.getLoginId())) {
            throw new GlobalException(ALREADY_LOGIN_ID);
        }

        // 닉네임 확인
        if (signUp.getUserNickName().equals("")) {
            throw new GlobalException(BLANK_NICK_NAME);
        } else if (userRepository.existsByUserNickName(signUp.getUserNickName())) {
            throw new GlobalException(ALREADY_NICK_NAME);
        }

        // 비밀번호 확인
        if (signUp.getUserPassword().equals("")) {
            throw new GlobalException(BLANK_PASSWORD);
        }

        signUp.setUserPassword(passwordEncoder.encode(signUp.getUserPassword()));
        userRepository.save(signUp.save(signUp));

        return "회원가입이 완료 되었습니다.";
    }


    // 로그인
    public String userSignIn(UserDto.SignIn signIn) {

        // 입력값 확인
        if (signIn.getLoginId().equals("")) {
            throw new GlobalException(BLANK_LOGIN_ID);
        }
        if (signIn.getUserPassword().equals("")) {
            throw new GlobalException(BLANK_PASSWORD);
        }

        User user = userRepository.findByLoginId(signIn.getLoginId())
            .orElseThrow(() -> new GlobalException(NOT_FIND_LOGIN_ID));

        if (!passwordEncoder.matches(signIn.getUserPassword(), user.getUserPassword())) {
            throw new GlobalException(WRONG_LOGIN_PASSWORD);
        }

        return tokenProvider.createToken(user.getLoginId(), user.getUserRank());
    }
}