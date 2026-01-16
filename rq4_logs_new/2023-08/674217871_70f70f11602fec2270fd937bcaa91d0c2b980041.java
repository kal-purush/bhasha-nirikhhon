package api.wantedpreonboardingbackend.domain.member.controller;

import api.wantedpreonboardingbackend.domain.member.entity.Member;
import api.wantedpreonboardingbackend.domain.member.repository.MemberRepository;
import api.wantedpreonboardingbackend.domain.post.controller.PostController;
import api.wantedpreonboardingbackend.global.dto.CustomFailureCode;
import api.wantedpreonboardingbackend.global.dto.CustomSuccessCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.handler;

@SpringBootTest
@AutoConfigureMockMvc
@Transactional
@ActiveProfiles("test")
@TestMethodOrder(MethodOrderer.MethodName.class)
class MemberControllerTest {
    @Autowired
    private MockMvc mvc;
    @Autowired
    private MemberRepository memberRepository;
    @Autowired
    private PasswordEncoder passwordEncoder;

    @Test
    @DisplayName("회원 가입 성공 테스트")
    void joinSuccessTest() throws Exception {
        // When
        ResultActions resultActions = mvc
                .perform(
                        post("/api/member")
                                .content("""
                                        {
                                            "email": "user1@test.com",
                                            "password": "12345678"
                                        }
                                        """.stripIndent())
                                .contentType(new MediaType(MediaType.APPLICATION_JSON, StandardCharsets.UTF_8))
                )
                .andDo(print());

        // Then
        resultActions
                .andExpect(status().is2xxSuccessful())
                .andExpect(handler().handlerType(MemberController.class))
                .andExpect(handler().methodName("join"))
                .andExpect(jsonPath("$.resultCode").value(CustomSuccessCode.S_101.getCode()))
                .andExpect(jsonPath("$.message").value(CustomSuccessCode.S_101.getMessage()))
                .andExpect(jsonPath("$.data.member.id").value(1))
                .andExpect(jsonPath("$.data.member.email").value("user1@test.com"))
        ;
    }

    @Test
    @DisplayName("회원 가입 실패 테스트1 - @ 미포함 이메일")
    void joinFailureTest1() throws Exception {
        // When
        ResultActions resultActions = mvc
                .perform(
                        post("/api/member")
                                .content("""
                                        {
                                            "email": "user1",
                                            "password": "12345678"
                                        }
                                        """.stripIndent())
                                .contentType(new MediaType(MediaType.APPLICATION_JSON, StandardCharsets.UTF_8))
                )
                .andDo(print());

        // Then
        resultActions
                .andExpect(status().is4xxClientError())
                .andExpect(handler().handlerType(MemberController.class))
                .andExpect(handler().methodName("join"))
                .andExpect(jsonPath("$.resultCode").value(CustomFailureCode.F_105.getCode()))
                .andExpect(jsonPath("$.message").value(CustomFailureCode.F_105.getMessage()))
        ;
    }

    @Test
    @DisplayName("회원 가입 실패 테스트2 - 8자 미만의 비밀번호")
    void joinFailureTest2() throws Exception {
        // When
        ResultActions resultActions = mvc
                .perform(
                        post("/api/member")
                                .content("""
                                        {
                                            "email": "user1@test.com",
                                            "password": "1234"
                                        }
                                        """.stripIndent())
                                .contentType(new MediaType(MediaType.APPLICATION_JSON, StandardCharsets.UTF_8))
                )
                .andDo(print());

        // Then
        resultActions
                .andExpect(status().is4xxClientError())
                .andExpect(handler().handlerType(MemberController.class))
                .andExpect(handler().methodName("join"))
                .andExpect(jsonPath("$.resultCode").value(CustomFailureCode.F_106.getCode()))
                .andExpect(jsonPath("$.message").value(CustomFailureCode.F_106.getMessage()))
        ;
    }

    @Test
    @DisplayName("로그인 성공 테스트")
    void loginSuccessTest() throws Exception {
        // Given
        Member member = Member.builder()
                .email("user1@test.com")
                .password(passwordEncoder.encode("12341234"))
                .build();
        memberRepository.save(member);

        // When
        ResultActions resultActions = mvc
                .perform(
                        post("/api/member/login")
                                .content("""
                                        {
                                            "email": "user1@test.com",
                                            "password": "12341234"
                                        }
                                        """.stripIndent())
                                .contentType(new MediaType(MediaType.APPLICATION_JSON, StandardCharsets.UTF_8))
                )
                .andDo(print());

        // Then
        resultActions
                .andExpect(status().is2xxSuccessful())
                .andExpect(handler().handlerType(MemberController.class))
                .andExpect(handler().methodName("login"))
                .andExpect(jsonPath("$.resultCode").value(CustomSuccessCode.S_102.getCode()))
                .andExpect(jsonPath("$.message").value(CustomSuccessCode.S_102.getMessage()))
                .andExpect(jsonPath("$.data.jwtToken").exists())
        ;
    }

    @Test
    @DisplayName("로그인 실패 테스트1 - 이메일 오류")
    void loginFailureTest1() throws Exception {
        // Given
        Member member = Member.builder()
                .email("user1@test.com")
                .password(passwordEncoder.encode("12341234"))
                .build();
        memberRepository.save(member);

        // When
        ResultActions resultActions = mvc
                .perform(
                        post("/api/member/login")
                                .content("""
                                        {
                                            "email": "user2@test.com",
                                            "password": "12341234"
                                        }
                                        """.stripIndent())
                                .contentType(new MediaType(MediaType.APPLICATION_JSON, StandardCharsets.UTF_8))
                )
                .andDo(print());

        // Then
        resultActions
                .andExpect(status().is4xxClientError())
                .andExpect(handler().handlerType(MemberController.class))
                .andExpect(handler().methodName("login"))
                .andExpect(jsonPath("$.resultCode").value(CustomFailureCode.F_102.getCode()))
                .andExpect(jsonPath("$.message").value(CustomFailureCode.F_102.getMessage()))
        ;
    }

    @Test
    @DisplayName("로그인 실패 테스트2 - 비밀번호 오류")
    void loginFailureTest2() throws Exception {
        // Given
        Member member = Member.builder()
                .email("user1@test.com")
                .password(passwordEncoder.encode("12341234"))
                .build();
        memberRepository.save(member);

        // When
        ResultActions resultActions = mvc
                .perform(
                        post("/api/member/login")
                                .content("""
                                        {
                                            "email": "user1@test.com",
                                            "password": "12341235"
                                        }
                                        """.stripIndent())
                                .contentType(new MediaType(MediaType.APPLICATION_JSON, StandardCharsets.UTF_8))
                )
                .andDo(print());

        // Then
        resultActions
                .andExpect(status().is4xxClientError())
                .andExpect(handler().handlerType(MemberController.class))
                .andExpect(handler().methodName("login"))
                .andExpect(jsonPath("$.resultCode").value(CustomFailureCode.F_103.getCode()))
                .andExpect(jsonPath("$.message").value(CustomFailureCode.F_103.getMessage()))
        ;
    }
}