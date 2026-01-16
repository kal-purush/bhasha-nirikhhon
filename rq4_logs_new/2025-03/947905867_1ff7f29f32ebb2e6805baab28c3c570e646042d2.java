package com.pet.pet_funeral.domain.pet_funeral.service;

import com.pet.pet_funeral.security.jwt.JwtService;
import com.pet.pet_funeral.security.service.CookieService;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.BDDMockito;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpRequest;
import org.springframework.http.ResponseCookie;

import java.util.Optional;

import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class CookieServiceTest {

    @InjectMocks
    private CookieService cookieService;

    private final String REFRESH_TOKEN = "refreshToken";

    @Test
    @DisplayName("리프레시토큰쿠키 생성 성공")
    void 리프레시토큰쿠키_생성_성공 () throws Exception {
        //given
        //when
        ResponseCookie refreshTokenCookie = cookieService.createRefreshTokenCookie(REFRESH_TOKEN);

        //then
        Assertions.assertThat(refreshTokenCookie.getName()).isEqualTo("refreshToken");
    }

    @Test
    @DisplayName("리프레시토큰 쿠키에서 추출 성공")
    void 쿠키에서_토큰_추출_성공 () throws Exception {
        //given
        HttpServletRequest request = mock(HttpServletRequest.class);
        Cookie[] cookies = {
                new Cookie("cookie","value"),
                new Cookie("refreshToken","test-refreshToken")
        };

        BDDMockito.given(request.getCookies()).willReturn(cookies);
        //when
        Optional<String> cookie = cookieService.getRefreshTokenFromCookie(request);
        //then
        Assertions.assertThat(cookie).isPresent();
        Assertions.assertThat(cookie.get()).isEqualTo("test-refreshToken");
    }
}