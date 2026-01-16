package jaeseok.numble.mybox.member.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import jaeseok.numble.mybox.member.dto.SignUpParam;
import jaeseok.numble.mybox.storage.StorageHandler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.transaction.annotation.Transactional;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Transactional
class MemberControllerTest {

    @MockBean
    protected StorageHandler storageHandler;

    @Autowired
    protected MockMvc mockMvc;

    @Autowired
    protected ObjectMapper objectMapper;

    @Nested
    @DisplayName("회원가입")
    class SignUpTest {
        private String email = "abc@def.ghi";
        private String password = "jklmn!@#$%";

        @Test
        @DisplayName("성공")
        void success() throws Exception {
            // given
            SignUpParam signUpParam = new SignUpParam(email, password);

            // when
            ResultActions resultActions = mockMvc.perform(post("/api/v1/member")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(signUpParam))
                    .accept(MediaType.APPLICATION_JSON))
                    .andDo(print());

            // then
            resultActions
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("content.id").isNumber())
                    .andExpect(jsonPath("content.email").value(email));
        }
    }
}