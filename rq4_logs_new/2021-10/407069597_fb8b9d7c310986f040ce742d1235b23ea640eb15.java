package com.bithumb.interest.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.bithumb.interest.api.dto.InterestResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;


@ExtendWith(SpringExtension.class)
@AutoConfigureMockMvc
@SpringBootTest
@Transactional
class InterestControllerTest {

    @Autowired
    protected MockMvc mockMvc;
    @Autowired
    protected ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
    }

    @Test
    void getInterests() throws Exception {
        mockMvc.perform(get("/interests/1")
                        .contentType(MediaType.APPLICATION_JSON))
                .andDo(print())
                .andExpect(status().isOk());
        //.andExpect(jsonPath("message").value(SuccessCode.USER_UPDATE_PASSWORD_SUCESS.getMessage()));
    }

    @Test
    void createInterest() throws Exception {
        String json = objectMapper.writeValueAsString("BTC");

        mockMvc.perform(post("/interest/1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andDo(print())
                .andExpect(status().isOk());
        //.andExpect(jsonPath("message").value(SuccessCode.USER_UPDATE_PASSWORD_SUCESS.getMessage()));

    }

    @Test
    void deleteInterest() throws Exception {
        String json = objectMapper.writeValueAsString("BTC");

        mockMvc.perform(delete("/interest/1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andDo(print())
                .andExpect(status().isOk());
        //.andExpect(jsonPath("message").value(SuccessCode.USER_UPDATE_PASSWORD_SUCESS.getMessage()));

    }
}