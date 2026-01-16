package com.example.lifeonhana.controller;

import static org.mockito.ArgumentMatchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.math.BigDecimal;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import com.example.lifeonhana.dto.response.LifeProductResponseDTO;
import com.example.lifeonhana.dto.response.LoanProductDetailResponseDTO;
import com.example.lifeonhana.dto.response.ProductListResponseDTO;
import com.example.lifeonhana.dto.response.ProductResponseDTO;
import com.example.lifeonhana.dto.response.SavingProductResponseDTO;
import com.example.lifeonhana.entity.User;
import com.example.lifeonhana.repository.UserRepository;
import com.example.lifeonhana.service.JwtService;
import com.example.lifeonhana.service.ProductService;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootTest(
	properties = "spring.profiles.active=test",
	webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ActiveProfiles("test")
public class ProductControllerTest {
	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private UserRepository userRepository;

	@Autowired
	private JwtService jwtService;

	private String validToken;

	@MockBean
	private ProductService productService;

	@BeforeEach
	void setUp() {
		User user = userRepository.findById(3L).orElseThrow(() -> new RuntimeException("테스트 사용자가 없습니다."));
		validToken = "Bearer " + jwtService.generateAccessToken("user1@example.com", 3L);
		System.out.println("validToken = " + validToken);
	}

	@Test
	@Order(1)
	@DisplayName("상품 목록 조회 - 성공")
	void getProductList() throws Exception {
		ProductResponseDTO product1 = new ProductResponseDTO(
			1L, "Product 1", "Description 1", "SAVINGS",
			BigDecimal.valueOf(1000), BigDecimal.valueOf(5000),
			BigDecimal.valueOf(1.5), BigDecimal.valueOf(2.0),
			12, 600L
		);

		ProductResponseDTO product2 = new ProductResponseDTO(
			2L, "Product 2", "Description 2", "LOAN",
			BigDecimal.valueOf(2000), BigDecimal.valueOf(10000),
			BigDecimal.valueOf(3.0), BigDecimal.valueOf(4.5),
			24, 700L
		);

		ProductListResponseDTO<ProductResponseDTO> response = new ProductListResponseDTO<>(
			List.of(product1, product2), true
		);

		Mockito.doReturn(response).when(productService).getProducts("SAVINGS", 1, 20);

		mockMvc.perform(get("/api/products")
				.param("category", "SAVINGS")
				.param("offset", "1")
				.param("limit", "20")
				.header("Authorization", validToken))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.code").value(200))
			.andExpect(jsonPath("$.status").value("OK"))
			.andExpect(jsonPath("$.message").value("상품 목록 조회 성공"))
			.andExpect(jsonPath("$.data.products[0].name").value("Product 1"))
			.andExpect(jsonPath("$.data.products[1].name").value("Product 2"))
			.andExpect(jsonPath("$.data.hasNext").value(true));
	}

	@Test
	@Order(2)
	@DisplayName("예적금 상품 상세 조회 - 성공")
	void getSavingProduct() throws Exception {
		SavingProductResponseDTO response = new SavingProductResponseDTO(
			1L, "Savings Product", "Description", "https://example.com", true,
			new SavingProductResponseDTO.SavingsInfo(
				BigDecimal.valueOf(1.5),
				BigDecimal.valueOf(2.0)
			)
		);
		Mockito.when(productService.getSavingsProduct(eq(1L)))
			.thenReturn(response);

		// When & Then
		mockMvc.perform(get("/api/products/savings/1")
				.header("Authorization", validToken))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.code").value(200))
			.andExpect(jsonPath("$.message").value("예적금 상품 상세 조회 성공"))
			.andExpect(jsonPath("$.data.name").value("Savings Product"))
			.andExpect(jsonPath("$.data.isLike").value(true))
			.andExpect(jsonPath("$.data.savingsInfo.basicInterestRate").value(1.5));
	}

	@Test
	@Order(3)
	@DisplayName("대출 상품 상세 조회 - 성공")
	void getLoanProduct() throws Exception {
		LoanProductDetailResponseDTO response = new LoanProductDetailResponseDTO(
			2L, "Loan Product", "Description", "Feature", "Target",
			"https://example.com", false,
			new LoanProductDetailResponseDTO.LoanInfo(
				BigDecimal.valueOf(2000),
				BigDecimal.valueOf(10000),
				BigDecimal.valueOf(3.0),
				BigDecimal.valueOf(4.5),
				12, 24, 600L
			)
		);

		Mockito.when(productService.getLoanProduct(eq(2L)))
			.thenReturn(response);

		// When & Then
		mockMvc.perform(get("/api/products/loans/2")
				.header("Authorization", validToken))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.code").value(200))
			.andExpect(jsonPath("$.message").value("대출 상품 상세 조회 성공"))
			.andExpect(jsonPath("$.data.name").value("Loan Product"))
			.andExpect(jsonPath("$.data.loanInfo.minAmount").value(2000));
	}

	@Test
	@Order(4)
	@DisplayName("라이프 상품 상세 조회 - 성공")
	void getLifeProduct() throws Exception {
		LifeProductResponseDTO response = new LifeProductResponseDTO(
			3L, "Life Product", "Description", "https://example.com", true
		);

		Mockito.when(productService.getLifeProduct(eq(3L)))
			.thenReturn(response);

		// When & Then
		mockMvc.perform(get("/api/products/life/3")
				.header("Authorization", validToken))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.code").value(200))
			.andExpect(jsonPath("$.message").value("라이프 상품 상세 조회 성공"))
			.andExpect(jsonPath("$.data.name").value("Life Product"))
			.andExpect(jsonPath("$.data.isLike").value(true));
	}
}