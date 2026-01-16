package com.example.lifeonhana.controller;
import com.example.lifeonhana.entity.User;
import com.example.lifeonhana.repository.UserRepository;
import com.example.lifeonhana.service.JwtService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.util.Map;

@SpringBootTest(
	properties = "spring.profiles.active=test",
	webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ActiveProfiles("test")
@Transactional
class LoanRecommendationControllerTest {

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private JwtService jwtService;

	@Autowired
	private UserRepository userRepository;

	private String validToken;

	@BeforeEach
	void setUp() {
		User testUser = userRepository.findById(3L)
			.orElseThrow(() -> new RuntimeException("테스트 사용자가 없습니다."));
		validToken = "Bearer " + jwtService.generateAccessToken(testUser.getAuthId(), testUser.getUserId());
	}

	@Test
	@DisplayName("대출 상품 추천 - 성공")
	void recommendLoanProducts_Success() throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		Map<String, Object> requestData = Map.of(
			"reason", "자녀 학자금",
			"amount", 5000000
		);

		mockMvc.perform(post("/api/anthropic/loans")
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(requestData))
				.header("Authorization", validToken))
			.andDo(print())
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.code").value(200))
			.andExpect(jsonPath("$.message").value("대출 상품 추천 성공"))
			.andExpect(jsonPath("$.data").isArray())
			.andExpect(jsonPath("$.data[0].productId").exists());
	}

	@Test
	@DisplayName("대출 상품 추천 - 잘못된 요청 (이유 누락)")
	void recommendLoanProducts_BadRequest_MissingReason() throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		Map<String, Object> invalidRequestData = Map.of(
			"amount", 5000000
		);

		mockMvc.perform(post("/api/anthropic/loans")
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(invalidRequestData))
				.header("Authorization", validToken))
			.andDo(print())
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(400))
			.andExpect(jsonPath("$.message").value("대출 사유는 필수 항목입니다."));
	}

	@Test
	@DisplayName("대출 상품 추천 - 잘못된 요청 (금액 누락)")
	void recommendLoanProducts_BadRequest_MissingAmount() throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		Map<String, Object> invalidRequestData = Map.of(
			"reason", "자녀 학자금"
		);

		mockMvc.perform(post("/api/anthropic/loans")
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(invalidRequestData))
				.header("Authorization", validToken))
			.andDo(print())
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(400))
			.andExpect(jsonPath("$.message").value("대출 금액은 0보다 커야 합니다."));
	}

	@Test
	@DisplayName("대출 상품 추천 - 인증 실패")
	void recommendLoanProducts_Unauthorized() throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		Map<String, Object> requestData = Map.of(
			"reason", "자녀 학자금",
			"amount", 5000000
		);

		mockMvc.perform(post("/api/anthropic/loans")
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(requestData)))
			.andDo(print())
			.andExpect(status().isUnauthorized())
			.andExpect(jsonPath("$.code").value(401))
			.andExpect(jsonPath("$.message").exists());
	}

	// @Test
	// @DisplayName("대출 상품 추천 - 내부 서버 오류")
	// void recommendLoanProducts_InternalServerError() throws Exception {
	// 	ObjectMapper objectMapper = new ObjectMapper();
	// 	Map<String, Object> requestData = Map.of(
	// 		"reason", "구매 자금",
	// 		"amount", -1
	// 	);
	//
	// 	mockMvc.perform(post("/api/anthropic/loans")
	// 			.contentType(MediaType.APPLICATION_JSON)
	// 			.content(objectMapper.writeValueAsString(requestData))
	// 			.header("Authorization", validToken))
	// 		.andDo(print())
	// 		.andExpect(status().isInternalServerError())
	// 		.andExpect(jsonPath("$.code").value(500))
	// 		.andExpect(jsonPath("$.message").exists());
	// }
}