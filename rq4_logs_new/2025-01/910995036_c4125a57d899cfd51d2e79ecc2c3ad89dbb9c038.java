package com.example.lifeonhana.controller;

import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.math.BigDecimal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import com.example.lifeonhana.dto.request.LumpSumRequestDTO;
import com.example.lifeonhana.dto.response.LumpSumResponseDTO;
import com.example.lifeonhana.entity.Account;
import com.example.lifeonhana.entity.LumpSum;
import com.example.lifeonhana.entity.User;
import com.example.lifeonhana.repository.AccountRepository;
import com.example.lifeonhana.repository.UserRepository;
import com.example.lifeonhana.service.JwtService;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootTest(
	properties = "spring.profiles.active=test",
	webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ActiveProfiles("test")
public class LumpSumControllerTest {

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private UserRepository userRepository;

	@Autowired
	private JwtService jwtService;

	@Autowired
	private AccountRepository accountRepository;

	private String validToken;

	@BeforeEach
	void setUp() {
		User user = userRepository.findById(3L).orElseThrow(() -> new RuntimeException("테스트 사용자가 없습니다."));
		validToken = "Bearer " + jwtService.generateAccessToken("user1@example.com", 3L);
		System.out.println("validToken = " + validToken);

		Account account = accountRepository.findByAccountId(11L).orElseThrow(() -> new RuntimeException("존재하지 않는 계좌 "
			+ "id 입니다."));
	}

	@Test
	@Order(1)
	@DisplayName("목돈 신청 성공")
	void createLumpSum() throws Exception {
		LumpSumRequestDTO requestDTO = new LumpSumRequestDTO(
			BigDecimal.valueOf(1000), LumpSum.Source.SALARY, LumpSum.Reason.CHILDREN, null, 11L);

		mockMvc.perform(MockMvcRequestBuilders.post("/api/lumpsum")
					.header("Authorization", validToken)
			.contentType(MediaType.APPLICATION_JSON)
			.content(objectMapper.writeValueAsString(requestDTO)))
			.andExpect(status().isOk())
			.andDo(print());
	}

}