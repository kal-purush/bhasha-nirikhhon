package com.aeon.hadog.controller;

import com.aeon.hadog.base.dto.user.JoinRequestDTO;
import com.aeon.hadog.base.dto.user.LoginRequestDTO;
import com.aeon.hadog.service.UserService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;


import static org.awaitility.Awaitility.given;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class UserControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Mock
    UserService userService;


    @Test
    @DisplayName("회원가입 테스트")
    void register() throws Exception {
        // given
        JoinRequestDTO dto = JoinRequestDTO.builder()
                .name("김민수")
                .id("minsu01")
                .password("minsu01@")
                .nickname("김민수01")
                .email("minsu01@gmail.com")
                .build();

        String json = new ObjectMapper().writeValueAsString(dto);

        // when
        ResultActions resultActions = mockMvc.perform(post("/user")
                .with(csrf())
                .contentType(MediaType.APPLICATION_JSON)
                .content(json));

        // then
        resultActions.andExpect(status().isOk());
    }

    @Test
    @DisplayName("로그인 테스트")
    void login() throws Exception {
        // given
        LoginRequestDTO dto = LoginRequestDTO.builder()
                .id("minsu01")
                .password("minsu01@")
                .build();

        String json = new ObjectMapper().writeValueAsString(dto);

        // when
        ResultActions resultActions = mockMvc.perform(post("/user/login")
                .with(csrf())
                .contentType(MediaType.APPLICATION_JSON)
                .content(json));

        // then
        resultActions.andExpect(status().isOk())
                .andExpect(jsonPath("$.data").exists());
    }

    @Test
    void checkId() throws Exception {
        // given
        String id = "minsu01";

        // when
        ResultActions resultActions = mockMvc.perform(get("/user/id").param("id", id));

        // then
        resultActions.andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(200))
                .andExpect(jsonPath("$.data").value(true))
                .andExpect(jsonPath("$.message").value("id값 중복 확인"));
    }

    @Test
    void checkNickName() throws Exception {
        // given
        String nickName = "김민수01";

        // when
        ResultActions resultActions = mockMvc.perform(get("/user/nickName").param("nickName", nickName));

        // then
        resultActions.andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(200))
                .andExpect(jsonPath("$.data").value(true))
                .andExpect(jsonPath("$.message").value("닉네임 중복 확인"));
    }

    @Test
    void checkEmail() throws Exception {
        // given
        String email = "minsu01@gmail.com";

        // when
        ResultActions resultActions = mockMvc.perform(get("/user/email").param("email", email));

        // then
        resultActions.andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(200))
                .andExpect(jsonPath("$.data").value(true))
                .andExpect(jsonPath("$.message").value("email 중복 확인"));
    }

    @Test
    void modifyPassword() throws Exception {
//        // given
//        String id = "minsu01";
//        String password = "minsu01@";
//        String newPassword = "minsu1234@";
//
//        // when
//        ResultActions resultActions = mockMvc.perform(patch("/user/password")
//                .header("Authorization", "Bearer " + token)
//                .contentType(MediaType.APPLICATION_JSON)
//                .param("newPassword", newPassword));
//
//        // then
//        resultActions.andExpect(status().isOk())
//                .andExpect(jsonPath("$.status").value(200))
//                .andExpect(jsonPath("$.data").value(true))
//                .andExpect(jsonPath("$.message").value("패스워드 변경 완료"));
    }

    @Test
    void deleteUser() {
        // given

        // when

        // then
    }
}