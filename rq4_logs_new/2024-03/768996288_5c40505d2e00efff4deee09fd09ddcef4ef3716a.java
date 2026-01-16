package api.auth;

import api.auth.dto.SignUpDto;
import api.user.User;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
@Transactional
@SpringBootTest
@ActiveProfiles("test")
public class AuthServiceTest {

    @Autowired
    private AuthService authService;

    @Test
    @DisplayName("회원 가입")
    public void signUp() {
        SignUpDto signUpDto = new SignUpDto("test@example.com", User.UserType.OWNER, "Abcd123@", "01012345678", User.Gender.MALE, LocalDateTime.of(2000, 1, 1, 0, 0));
        SignUpDto result = authService.signUp(signUpDto);
        assertThat(result).isNotNull();
        assertThat(result.getEmail()).isEqualTo("test@example.com");
    }

    @Test
    @DisplayName("중복 이메일 회원가입 시도 에러 테스트")
    public void duplicatedEmail() {
        SignUpDto signUpDto1 = new SignUpDto("test@example.com", User.UserType.OWNER, "Abcd123@", "01012345678", User.Gender.MALE, LocalDateTime.now());
        authService.signUp(signUpDto1);
        SignUpDto signUpDto2 = new SignUpDto("test@example.com", User.UserType.OWNER, "Efgh456#", "01087654321", User.Gender.FEMALE, LocalDateTime.now());
        assertThrows(IllegalStateException.class, () -> authService.signUp(signUpDto2));
    }

    @Test
    @DisplayName("이메일 형식 에러 테스트")
    public void emailFormat() {
        SignUpDto signUpDto = new SignUpDto("invalid_email_format", User.UserType.OWNER, "Abcd123@", "01012345678", User.Gender.MALE, LocalDateTime.now());
        assertThrows(IllegalArgumentException.class, () -> authService.signUp(signUpDto));
    }

    @Test
    @DisplayName("비밀번호 형식 테스트")
    public void pwFormat() {
        SignUpDto signUpDto = new SignUpDto("test@example.com", User.UserType.OWNER, "invalid_pw_format", "01012345678", User.Gender.MALE, LocalDateTime.now());
        assertThrows(IllegalArgumentException.class, () -> authService.signUp(signUpDto));
    }
}