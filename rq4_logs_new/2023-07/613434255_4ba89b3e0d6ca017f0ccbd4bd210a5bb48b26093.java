package jaeseok.numble.mybox.file.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import jaeseok.numble.mybox.common.response.MyBoxResponse;
import jaeseok.numble.mybox.common.response.ResponseCode;
import jaeseok.numble.mybox.member.dto.LoginParam;
import jaeseok.numble.mybox.member.dto.LoginResponse;
import jaeseok.numble.mybox.member.dto.SignUpParam;
import jaeseok.numble.mybox.util.annotation.IntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@IntegrationTest
class FileControllerTest {

    @Autowired
    protected MockMvc mockMvc;

    @Autowired
    protected ObjectMapper objectMapper;

    @Nested
    @DisplayName("파일 업로드")
    class Upload {

        private String email = "";
        private String password = "";
        private String jwt;

        @BeforeEach
        void initMemberAndJwt() throws Exception {
            // 회원가입
            SignUpParam signUpParam = new SignUpParam(email, password);
            mockMvc.perform(post("/api/v1/member")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(signUpParam))
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(print());

            // 로그인
            LoginParam loginParam = new LoginParam(email, password);
            ResultActions resultActions = mockMvc.perform(post("/api/v1/login")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(loginParam))
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(print());

            String content = resultActions.andReturn().getResponse().getContentAsString();
            MyBoxResponse myBoxResponse = objectMapper.readValue(content, MyBoxResponse.class);

            String json = objectMapper.writeValueAsString(myBoxResponse.getContent());
            LoginResponse loginResponse = objectMapper.readValue(json, LoginResponse.class);

            jwt = loginResponse.getJwt();
        }

//        @DisplayName("업로드 성공")
//        void success() {
//            // given
//            SignUpParam signUpParam = new SignUpParam(email, password);
//
//            // when
//            ResultActions resultActions = mockMvc.perform(post("/v1/{parentId}/file")
//                            .contentType(MediaType.APPLICATION_JSON)
//                            .content(objectMapper.writeValueAsString(signUpParam))
//                            .accept(MediaType.APPLICATION_JSON))
//                    .andDo(print());
//
//            // then
//            resultActions
//                    .andExpect(status().isOk())
//                    .andExpect(jsonPath("success").value(true))
//                    .andExpect(jsonPath("errorCode").value(ResponseCode.SUCCESS.getCode()))
//                    .andExpect(jsonPath("content.id").isNumber())
//                    .andExpect(jsonPath("content.email").value(email));
//        }
    }
}