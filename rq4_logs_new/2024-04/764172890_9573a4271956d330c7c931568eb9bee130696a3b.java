package com.aeon.hadog.controller;

import com.aeon.hadog.base.dto.PetDTO;
import com.aeon.hadog.base.dto.user.JoinRequestDTO;
import com.aeon.hadog.base.dto.user.LoginRequestDTO;
import com.aeon.hadog.service.PetService;
import com.aeon.hadog.service.UserService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.http.ResponseEntity.status;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;

// 403 오류 수정 필요

@WebMvcTest(PetController.class)
class PetControllerTest {

    @Autowired
    MockMvc mvc;

    @MockBean
    PetService petService;

    @MockBean
    UserService userService;

    @BeforeEach
    public void before() {
        // 회원가입
        JoinRequestDTO user = new JoinRequestDTO();
        user.setId("user");
        user.setName("홍길동");
        user.setNickname("유저");
        user.setPassword("Pw1234!!");
        user.setEmail("user123@gmail.com");
        userService.signup(user);
        // 로그인
        LoginRequestDTO loginUser = new LoginRequestDTO();
        loginUser.setId("user");
        loginUser.setPassword("Pw1234!!");
        userService.signin(loginUser);
    }

    @Test
    @DisplayName("반려견 등록")
    void register() throws Exception {
        // 로그인
        LoginRequestDTO loginUser = new LoginRequestDTO();
        loginUser.setId("user");
        loginUser.setPassword("Pw1234!!");
        userService.signin(loginUser);

        //given
        PetDTO petDTO = new PetDTO();
        petDTO.setAge(3);
        petDTO.setName("뽀삐");
        petDTO.setSex(true);
        petDTO.setNeuter(true);
        petDTO.setBreed("말티즈");

        //when
        petService.registerPet(loginUser.getId(), petDTO);

        //then
        mvc.perform(post("/pet/register"))
                .andDo(print());

    }

    @Test
    @DisplayName("반려견 정보 수정")
    @WithMockUser(username = "user", password = "Pw1234!!")
    void update() throws Exception {
        // 로그인
        LoginRequestDTO loginUser = new LoginRequestDTO();
        loginUser.setId("user");
        loginUser.setPassword("Pw1234!!");
        userService.signin(loginUser);

        //given
        PetDTO petDTO = new PetDTO();
        petDTO.setAge(4);
        petDTO.setName("뽀삐");
        petDTO.setSex(false);
        petDTO.setNeuter(true);
        petDTO.setBreed("말티즈");

        //when
        Long petId = petService.registerPet(loginUser.getId(), petDTO);

        //then
        mvc.perform(put("/update/"+petId))
                .andDo(print());
    }
}