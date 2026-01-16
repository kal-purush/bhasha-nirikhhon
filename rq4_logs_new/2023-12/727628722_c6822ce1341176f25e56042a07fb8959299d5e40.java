package com.spartanullnull.otil.domain.user.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.spartanullnull.otil.domain.user.dto.LoginRequestDto;
import com.spartanullnull.otil.domain.user.entity.User;
import com.spartanullnull.otil.domain.user.entity.UserRoleEnum;
import com.spartanullnull.otil.domain.user.repository.UserRepository;
import com.spartanullnull.otil.domain.user.service.LoginService;
import com.spartanullnull.otil.global.dto.ApiResponseDto;
import com.spartanullnull.otil.jwt.JwtUtil;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletResponse;

@SpringBootTest
public class LoginControllerTest {

    @Autowired
    LoginController loginController;

    @Autowired
    LoginService loginService;

    @Autowired
    UserRepository userRepository;

    @Autowired
    JwtUtil jwtUtil;

    LoginRequestDto loginRequestDto;

    HttpServletResponse response;

    User user = new User("asdf1234","asdf1234!@#","tony121","10eg@gmail.com", UserRoleEnum.USER);

    @BeforeEach
    void setUp() {
        response = new MockHttpServletResponse();
        if(userRepository.findByAccountId(user.getAccountId()).isEmpty()){
            userRepository.save(user);
        }
        loginRequestDto = new LoginRequestDto("asdf1234", "asdf1234!@#");
        UserRoleEnum role = user.getRole();
        response.setHeader(JwtUtil.AUTHORIZATION_HEADER, jwtUtil.createToken(user.getAccountId(), role));
    }

    @Test
    public void 로그인시도_해피케이스(){
        // GIVEN
        MockHttpServletResponse httpResponse = new MockHttpServletResponse();

        // WHEN
        ResponseEntity<ApiResponseDto> response = loginController.login(loginRequestDto, httpResponse);

        // THEN
        ApiResponseDto apiResponseDto = response.getBody();
        String msg = apiResponseDto.getMsg();
        int code = apiResponseDto.getStatusCode();

        System.out.println("apiResponseDto = " + apiResponseDto); //성공 시 id + 반환 코드 값만 반환
        System.out.println("loginRequestDto.getAccountId() = " + loginRequestDto.getAccountId());
        System.out.println("loginRequestDto.getPassword() = " + loginRequestDto.getPassword());
        assertEquals("로그인 성공",msg);
        assertEquals(200,code);
    }
}