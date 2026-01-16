package com.brainstrom.meokjang;

import com.brainstrom.meokjang.user.domain.User;
import com.brainstrom.meokjang.user.dto.request.LoginRequest;
import com.brainstrom.meokjang.user.dto.request.SignupRequest;
import com.brainstrom.meokjang.user.repository.UserRepository;
import com.brainstrom.meokjang.user.service.AuthService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
public class AuthServiceMockTest {

    @Mock
    private UserRepository userRepository;
    @InjectMocks
    private AuthService authService;

    @Test
    @DisplayName("mock을 이용한 회원가입 테스트")
    void join() {
        //given
        SignupRequest signupRequest = SignupRequest.builder()
                .userName("test")
                .phoneNumber("01012345678")
                .location("test")
                .latitude(0.0)
                .longitude(0.0)
                .gender(1)
                .reliability((float)50)
                .build();

        User user = User.builder()
                .userId(1L)
                .userName("test")
                .phoneNumber("01012345678")
                .location("test")
                .latitude(0.0)
                .longitude(0.0)
                .gender(1)
                .reliability((float)50)
                .build();

        Mockito.when(userRepository.save(Mockito.any(User.class))).thenReturn(user);
        Mockito.when(userRepository.findByPhoneNumber(Mockito.anyString())).thenReturn(Optional.of(user));
        Mockito.when(userRepository.findById(Mockito.any())).thenReturn(Optional.of(user));

        //when
        Long savedId = authService.join(signupRequest).getUserId();

        //then
        assertNotNull(savedId);
        assertEquals(userRepository.findByPhoneNumber("01012345678"), userRepository.findById(savedId));
    }

    @Test
    @DisplayName("mock을 이용한 로그인 테스트 - 전화번호")
    void login1() {
        //given
        SignupRequest signupRequest = SignupRequest.builder()
                .userName("test")
                .phoneNumber("01012345678")
                .location("test")
                .latitude(0.0)
                .longitude(0.0)
                .gender(1)
                .reliability((float)50)
                .build();

        LoginRequest loginRequest = LoginRequest.builder()
                .phoneNumber("01012345678")
                .build();

        User user = User.builder()
                .userId(1L)
                .userName("test")
                .phoneNumber("01012345678")
                .location("test")
                .latitude(0.0)
                .longitude(0.0)
                .gender(1)
                .reliability((float)50)
                .build();

        Mockito.when(userRepository.save(Mockito.any(User.class))).thenReturn(user);
        Mockito.when(userRepository.findByPhoneNumber(Mockito.anyString())).thenReturn(Optional.of(user));

        //when
        Long savedId = authService.join(signupRequest).getUserId();
        Long loginId = authService.login(loginRequest).getUserId();

        //then
        assertNotNull(savedId);
        assertEquals(savedId, loginId);
    }

    @Test
    @DisplayName("mock을 이용한 로그인 테스트 - SNS")
    void login2() {
        //given
        SignupRequest signupRequest = SignupRequest.builder()
                .userName("test")
                .snsType("naver")
                .snsKey("test")
                .location("test")
                .latitude(0.0)
                .longitude(0.0)
                .gender(1)
                .reliability((float)50)
                .build();

        LoginRequest loginRequest = LoginRequest.builder()
                .snsType("naver")
                .snsKey("test")
                .build();

        User user = User.builder()
                .userId(1L)
                .userName("test")
                .snsType("naver")
                .snsKey("test")
                .location("test")
                .latitude(0.0)
                .longitude(0.0)
                .gender(1)
                .reliability((float)50)
                .build();

        Mockito.when(userRepository.save(Mockito.any(User.class))).thenReturn(user);
        Mockito.when(userRepository.findBySns(Mockito.anyString(), Mockito.anyString())).thenReturn(Optional.of(user));

        //when
        Long savedId = authService.join(signupRequest).getUserId();
        Long loginId = authService.login(loginRequest).getUserId();

        //then
        assertNotNull(savedId);
        assertEquals(savedId, loginId);
    }
}