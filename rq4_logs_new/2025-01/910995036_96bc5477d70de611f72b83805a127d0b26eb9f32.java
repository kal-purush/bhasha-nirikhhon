package com.example.lifeonhana.controller;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.time.format.DateTimeFormatter;

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

import com.example.lifeonhana.dto.request.WalletRequestDTO;
import com.example.lifeonhana.dto.response.WalletResponseDTO;
import com.example.lifeonhana.entity.User;
import com.example.lifeonhana.entity.Wallet;
import com.example.lifeonhana.repository.UserRepository;
import com.example.lifeonhana.repository.WalletRepository;
import com.example.lifeonhana.service.JwtService;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootTest(
	properties = "spring.profiles.active=test",
	webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ActiveProfiles("test")
public class WalletControllerTest {
	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private UserRepository userRepository;

	@Autowired
	private JwtService jwtService;

	private String validToken;

	@Autowired
	private WalletRepository walletRepository;

	private User user;

	@BeforeEach
	void setUp() {
		user = userRepository.findById(3L).orElseThrow(() -> new RuntimeException("테스트 사용자가 없습니다."));
		validToken = "Bearer " + jwtService.generateAccessToken(user.getAuthId(), 3L);
	}

	@Test
	@Order(1)
	@DisplayName("하나지갑 정보 등록")
	void createWallet() throws Exception {
		walletRepository.deleteAll();

		WalletRequestDTO requestDTO = new WalletRequestDTO(
			330000000L, "15", "2023-01", "2023-12"
		);

		mockMvc.perform(post("/api/wallet")
				.header("Authorization", validToken)
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(requestDTO)))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.code").value(200))
			.andExpect(jsonPath("$.message").value("하나지갑 정보 등록 성공"))
			.andExpect(jsonPath("$.data.walletAmount").value(330000000))
			.andExpect(jsonPath("$.data.paymentDay").value("15"))
			.andExpect(jsonPath("$.data.startDate").value("2023-01"))
			.andExpect(jsonPath("$.data.endDate").value("2023-12"));

		assertThat(walletRepository.findWalletIdByUserAuthId(user.getAuthId())).isPresent();

	}

	@Test
	@Order(2)
	@DisplayName("하나지갑 정보 등록 실패")
	void createWalletFail() throws Exception {
		WalletRequestDTO requestDTO = new WalletRequestDTO(
			330000000L, "15", "2023-01", "2023-12"
		);

		mockMvc.perform(post("/api/wallet")
				.header("Authorization", validToken)
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(requestDTO)))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(400))
			.andExpect(jsonPath("$.message").value("Bad Request.\n이미 하나지갑 정보가 존재합니다."));
	}


	@Test
	@Order(3)
	@DisplayName("하나지갑 정보 조회")
	void getWallet() throws Exception {
		Wallet wallet = walletRepository.findWalletIdByUserAuthId(user.getAuthId()).orElseThrow(() -> new RuntimeException(
			"하나지갑 정보를 찾을 수 없습니다."));

		mockMvc.perform(get("/api/wallet")
			.header("Authorization", validToken))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.code").value(200))
			.andExpect(jsonPath("$.message").value("하나지갑 정보 조회 성공"))
			.andExpect(jsonPath("$.data.walletAmount").value(wallet.getWalletAmount()))
			.andExpect(jsonPath("$.data.startDate").value(wallet.getStartDate().format(
				DateTimeFormatter.ofPattern("yyyy-MM"))))
			.andDo(print());
	}

	@Test
	@Order(4)
	@DisplayName("하나지갑 정보 수정")
	void updateWallet() throws Exception {
		Wallet wallet = walletRepository.findWalletIdByUserAuthId(user.getAuthId()).orElseThrow(() -> new RuntimeException(
			"하나지갑 정보를 찾을 수 없습니다."));
		WalletRequestDTO requestDTO = new WalletRequestDTO(
			430000000L, "1", "2024-01", "2024-12"
		);
		WalletResponseDTO responseDTO = new WalletResponseDTO(
			wallet.getWalletId(), 430000000L, "1", "2024-01", "2024-12"
		);

		mockMvc.perform(put("/api/wallet")
			.header("Authorization", validToken)
			.contentType(MediaType.APPLICATION_JSON)
			.content(objectMapper.writeValueAsString(requestDTO)))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.code").value(200))
			.andExpect(jsonPath("$.message").value("하나지갑 정보 수정 성공"))
			.andExpect(jsonPath("$.data.walletAmount").value(responseDTO.walletAmount()))
			.andExpect(jsonPath("$.data.paymentDay").value(responseDTO.paymentDay()))
			.andExpect(jsonPath("$.data.startDate").value(responseDTO.startDate()))
			.andExpect(jsonPath("$.data.endDate").value(responseDTO.endDate()))
			.andDo(print());
	}
}