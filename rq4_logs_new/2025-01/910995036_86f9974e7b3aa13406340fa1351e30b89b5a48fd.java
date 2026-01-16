package com.example.lifeonhana.auth;

import com.example.lifeonhana.dto.AuthRequestDTO;
import com.example.lifeonhana.dto.AuthResponseDTO;
import com.example.lifeonhana.dto.RefreshTokenRequestDTO;
import com.example.lifeonhana.entity.User;
import com.example.lifeonhana.repository.UserRepository;
import com.example.lifeonhana.service.RedisService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AuthIntegrationTest {

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private UserRepository userRepository;

	@Autowired
	private PasswordEncoder passwordEncoder;

	@Autowired
	private RedisService redisService;

	@Autowired
	private ObjectMapper objectMapper;

	private User testUser;
	private static final String TEST_AUTH_ID = "testuser@example.com";
	private static final String TEST_PASSWORD = "password123";
	private static final String TEST_NAME = "Test User";
	private static final String TEST_BIRTHDAY = "1990-01-01";

	@BeforeAll
	void setUp() {
		// Create test user
		testUser = new User();
		testUser.setAuthId(TEST_AUTH_ID);
		testUser.setPassword(passwordEncoder.encode(TEST_PASSWORD));
		testUser.setName(TEST_NAME);
		testUser.setBirthday(TEST_BIRTHDAY);
		testUser.setIsFirst(true);
		testUser = userRepository.save(testUser);
	}

	@AfterAll
	void tearDown() {
		// Clean up test data
		userRepository.delete(testUser);
	}

	@Test
	@DisplayName("SignIn Success Test")
	void signInSuccess() throws Exception {
		// Prepare request
		AuthRequestDTO request = new AuthRequestDTO();
		request.setAuthId(TEST_AUTH_ID);
		request.setPassword(TEST_PASSWORD);

		// Perform sign in request
		MvcResult result = mockMvc.perform(post("/api/auth/signin")
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(request)))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.code").value(200))
			.andExpect(jsonPath("$.message").value("로그인 성공"))
			.andExpect(jsonPath("$.data.userId").value(testUser.getUserId().toString()))
			.andExpect(jsonPath("$.data.accessToken").exists())
			.andExpect(jsonPath("$.data.refreshToken").exists())
			.andReturn();

		// Extract tokens for further tests
		AuthResponseDTO response = objectMapper.readValue(
			objectMapper.readTree(result.getResponse().getContentAsString())
				.get("data")
				.toString(),
			AuthResponseDTO.class
		);
		assertNotNull(response.getAccessToken());
		assertNotNull(response.getRefreshToken());
	}

	@Test
	@DisplayName("SignIn Failure - Wrong Password Test")
	void signInFailureWrongPassword() throws Exception {
		AuthRequestDTO request = new AuthRequestDTO();
		request.setAuthId(TEST_AUTH_ID);
		request.setPassword("wrongpassword");

		mockMvc.perform(post("/api/auth/signin")
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(request)))
			.andExpect(status().isUnauthorized())
			.andExpect(jsonPath("$.message").value("Unauthorized.\n잘못된 비밀번호입니다."));
	}

	@Test
	@DisplayName("SignIn Failure - User Not Found Test")
	void signInFailureUserNotFound() throws Exception {
		AuthRequestDTO request = new AuthRequestDTO();
		request.setAuthId("nonexistent@example.com");
		request.setPassword(TEST_PASSWORD);

		mockMvc.perform(post("/api/auth/signin")
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(request)))
			.andExpect(status().isNotFound())
			.andExpect(jsonPath("$.message").value("Not Found.\n사용자를 찾을 수 없습니다."));
	}

	@Test
	@DisplayName("Refresh Token Test")
	void refreshTokenTest() throws Exception {
		// First, sign in to get tokens
		AuthRequestDTO signInRequest = new AuthRequestDTO();
		signInRequest.setAuthId(TEST_AUTH_ID);
		signInRequest.setPassword(TEST_PASSWORD);

		MvcResult signInResult = mockMvc.perform(post("/api/auth/signin")
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(signInRequest)))
			.andExpect(status().isOk())
			.andReturn();

		AuthResponseDTO signInResponse = objectMapper.readValue(
			objectMapper.readTree(signInResult.getResponse().getContentAsString())
				.get("data")
				.toString(),
			AuthResponseDTO.class
		);

		// Then, try to refresh token
		RefreshTokenRequestDTO refreshRequest = new RefreshTokenRequestDTO();
		refreshRequest.setRefreshToken(signInResponse.getRefreshToken());

		mockMvc.perform(post("/api/auth/refresh")
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(refreshRequest)))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.code").value(200))
			.andExpect(jsonPath("$.message").value("토큰 갱신 성공"))
			.andExpect(jsonPath("$.data.accessToken").exists())
			.andExpect(jsonPath("$.data.refreshToken").exists());
	}
	@Test
	@DisplayName("SignOut Success Test")
	void signOutSuccess() throws Exception {
		// First, sign in to get tokens
		AuthRequestDTO signInRequest = new AuthRequestDTO();
		signInRequest.setAuthId(TEST_AUTH_ID);
		signInRequest.setPassword(TEST_PASSWORD);

		MvcResult signInResult = mockMvc.perform(post("/api/auth/signin")
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(signInRequest)))
			.andExpect(status().isOk())
			.andReturn();

		AuthResponseDTO signInResponse = objectMapper.readValue(
			objectMapper.readTree(signInResult.getResponse().getContentAsString())
				.get("data")
				.toString(),
			AuthResponseDTO.class
		);

		// Then, try to sign out
		mockMvc.perform(post("/api/auth/signout")
				.header("Authorization", "Bearer " + signInResponse.getAccessToken()))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.code").value(200))
			.andExpect(jsonPath("$.message").value("로그아웃 성공"));

		// Verify token is blacklisted
		assertTrue(redisService.isBlacklisted(signInResponse.getAccessToken()));
	}

	// @Test
	// @DisplayName("SignOut Failure - Invalid Token Test")
	// void signOutFailureInvalidToken() throws Exception {
	// 	mockMvc.perform(post("/api/auth/signout")
	// 			.header("Authorization", "Bearer invalid_token"))
	// 		.andExpect(status().isBadRequest());
	// }
	//
	// @Test
	// public void RefreshTokenWithInvalidToken() throws Exception {
	// 	// Given - 잘못된 Refresh Token
	// 	RefreshTokenRequestDTO refreshRequest = RefreshTokenRequestDTO.builder()
	// 		.refreshToken("invalid.refresh.token")
	// 		.build();
	//
	// 	// When & Then
	// 	mockMvc.perform(post("/api/cert/refresh")
	// 			.contentType(MediaType.APPLICATION_JSON)
	// 			.content(objectMapper.writeValueAsString(refreshRequest)))
	// 		.andExpect(status().isUnauthorized());
	// }
}