package com.spartanullnull.otil.domain.user.controller;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.spartanullnull.otil.domain.user.entity.User;
import com.spartanullnull.otil.domain.user.service.UserProfileService;
import com.spartanullnull.otil.security.Impl.UserDetailsImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@WebMvcTest(controllers = UserProfileControllerTest.class)
//@ContextConfiguration(classes = {SecurityConfig.class})
//@ContextConfiguration(classes = {WebSecurityConfig.class, SecurityConfig.class})
class UserProfileControllerTest {

    @Autowired
    MockMvc mockMvc;

    @MockBean
    UserProfileService userProfileService;

    @Autowired
    WebApplicationContext webApplicationContext;

    @BeforeEach
    void setUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext)
            .apply(SecurityMockMvcConfigurers.springSecurity()).build();
    }//security 기능 사용하여 애플리케이션 test setup

    @Test
    @WithMockUser
    @DisplayName("PUT : 프로필 수정하기 요청을 처리합니다.")
    public void PUT_프로필수정_핸들링() {
        // GIVEN
        UserDetailsImpl userDetails = mock(UserDetailsImpl.class);
        User user = mock(User.class);
        when(userDetails.getUser()).thenReturn(user);

        // WHEN
        String accountId = userDetails.getUser().getAccountId();

        // THEN
        System.out.println("accountId = " + accountId);
    }
}