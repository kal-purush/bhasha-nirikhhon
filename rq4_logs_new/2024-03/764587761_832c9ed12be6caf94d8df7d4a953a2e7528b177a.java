package km.cd.backend.auth;

import km.cd.backend.global.error.CustomException;
import km.cd.backend.jwt.JwtTokenProvider;
import km.cd.backend.jwt.JwtTokenResponse;
import km.cd.backend.user.User;
import km.cd.backend.user.UserRepository;
import km.cd.backend.user.dto.UserLogin;
import km.cd.backend.user.dto.UserRegister;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AuthServiceTest {

    @InjectMocks
    AuthService authService;
    @Mock
    UserRepository userRepository;
    @Mock
    JwtTokenProvider jwtTokenProvider;
    @Spy
    BCryptPasswordEncoder passwordEncoder;

    @Test
    @DisplayName("일반_회원가입-성공")
    void register_success() {
        UserRegister userRegister = UserRegister.builder()
                .email("example@km.cd")
                .password("test12#$")
                .name("홍길동")
                .build();
        User fakeUser = User.builder()
                .email(userRegister.getEmail())
                .password(passwordEncoder.encode(userRegister.getPassword()))
                .name(userRegister.getName())
                .role("ROLE_USER") // 사용자 권한 설정
                .build();

        when(userRepository.findByEmail(eq(userRegister.getEmail())))
                .thenReturn(Optional.empty());
        when(userRepository.save(any(User.class)))
                .thenReturn(fakeUser);

        User user = authService.register(userRegister);
        assertEquals(user.getEmail(), userRegister.getEmail());
        assertEquals(user.getName(), userRegister.getName());
        assertTrue(passwordEncoder.matches(userRegister.getPassword(), user.getPassword()));
    }

    @Test
    @DisplayName("일반_회원가입-실패-중복이메일")
    void register_failed_duplicated_email() {
        UserRegister userRegister = UserRegister.builder()
                .email("example@km.cd")
                .password("test12#$")
                .name("홍길동")
                .build();
        User fakeUser = User.builder()
                .email(userRegister.getEmail())
                .build();

        doReturn(Optional.of(fakeUser)).when(userRepository).findByEmail(anyString());

        assertThrows(CustomException.class, () -> authService.register(userRegister));
    }

    @Test
    @DisplayName("일반_로그인-성공")
    void login_success() {
        UserLogin userLogin = UserLogin.builder()
                .email("example@km.cd")
                .password("test12#$")
                .build();
        User fakeUser = User.builder()
                .email(userLogin.getEmail())
                .password(passwordEncoder.encode(userLogin.getPassword()))
                .name("홍길동")
                .role("ROLE_USER") // 사용자 권한 설정
                .build();
        String expectedAccessToken = "expected_access_token";

        when(userRepository.findByEmail(anyString()))
                .thenReturn(Optional.of(fakeUser));
        when(jwtTokenProvider.generateAccessToken(
                eq(fakeUser.getEmail()),
                eq(fakeUser.getName()),
                anyCollection()
        )).thenReturn(expectedAccessToken);

        JwtTokenResponse tokenResponse = authService.login(userLogin);
        assertNotNull(tokenResponse);
        assertEquals(tokenResponse.getAccessToken(), expectedAccessToken);
    }

    @Test
    @DisplayName("일반_로그인-실패-유저_없음")
    void login_failed_user_not_found() {
        UserLogin userLogin = UserLogin.builder()
                .email("example@km.cd")
                .password("test12#$")
                .build();

        when(userRepository.findByEmail(eq(userLogin.getEmail())))
                .thenReturn(Optional.empty());

        assertThrows(CustomException.class, () -> authService.login(userLogin));
    }


    @Test
    @DisplayName("일반_로그인-실패-비밀번호_불일치")
    void login_failed_invalid_password() {
        UserLogin userLogin = UserLogin.builder()
                .email("example@km.cd")
                .password("test12#$")
                .build();
        User fakeUser = User.builder()
                .email(userLogin.getEmail())
                .password("wrong_password")
                .name("홍길동")
                .role("ROLE_USER") // 사용자 권한 설정
                .build();

        when(userRepository.findByEmail(eq(userLogin.getEmail())))
                .thenReturn(Optional.of(fakeUser));

        assertThrows(CustomException.class, () -> authService.login(userLogin));
    }

}