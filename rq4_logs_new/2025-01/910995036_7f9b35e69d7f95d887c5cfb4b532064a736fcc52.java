package com.example.lifeonhana.productInsight;

import com.example.lifeonhana.dto.request.ProductInsightRequest;
import com.example.lifeonhana.dto.response.ProductInsightResponse;
import com.example.lifeonhana.entity.Article;
import com.example.lifeonhana.entity.Product;
import com.example.lifeonhana.entity.User;
import com.example.lifeonhana.repository.ArticleRepository;
import com.example.lifeonhana.repository.ProductRepository;
import com.example.lifeonhana.repository.UserRepository;
import com.example.lifeonhana.service.JwtService;
import com.example.lifeonhana.service.ProductInsightService;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.transaction.Transactional;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Map;

@SpringBootTest(
	properties = "spring.profiles.active=test",
	webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ActiveProfiles("test")
@Transactional
class ProductInsightControllerTest {

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private JwtService jwtService;

	@Autowired
	private ArticleRepository articleRepository;

	@Autowired
	private ProductRepository productRepository;

	@Autowired
	private UserRepository userRepository;

	private String validToken;

	@BeforeEach
	void setUp() {
		// 실제 테스트 데이터로 토큰 생성
		User testUser = userRepository.findById(3L)
			.orElseThrow(() -> new RuntimeException("테스트 사용자가 없습니다."));
		validToken = "Bearer " + jwtService.generateAccessToken(testUser.getAuthId(), testUser.getUserId());
	}

	@Test
	@DisplayName("상품 기대효과 분석 조회 - 성공")
	@WithMockUser
	void getProductInsight_Success() throws Exception {
		// 테스트에 사용할 엔티티 조회
		Article article = articleRepository.findById(3L)
			.orElseThrow(() -> new RuntimeException("테스트 기사가 없습니다."));
		Product product = productRepository.findById(5L)
			.orElseThrow(() -> new RuntimeException("테스트 상품이 없습니다."));

		// 요청 데이터 생성
		ObjectMapper objectMapper = new ObjectMapper();
		Map<String, Object> requestData = Map.of(
			"articleId", article.getArticleId(),
			"productId", product.getProductId()
		);

		mockMvc.perform(post("/api/anthropic/effect")
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(requestData))
				.header("Authorization", validToken))
			.andDo(print()) // 응답 출력
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.code").value(200))
			.andExpect(jsonPath("$.message").value("상품 분석 성공"))
			.andExpect(jsonPath("$.data.analysisResult").exists())
			.andExpect(jsonPath("$.data.productLink").exists())
			.andExpect(jsonPath("$.data.productName").exists())
			.andExpect(jsonPath("$.data.isLiked").exists());
	}

	@Test
	@DisplayName("잘못된 요청 - articleId 누락")
	@WithMockUser
	void getProductInsight_BadRequest_MissingArticleId() throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		Map<String, Object> requestData = Map.of(
			"productId", 5L
		);

		mockMvc.perform(post("/api/anthropic/effect")
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(requestData))
				.header("Authorization", validToken))
			.andDo(print())
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(400))
			.andExpect(jsonPath("$.message").value("잘못된 요청입니다."));
	}

	@Test
	@DisplayName("서버 오류 발생")
	@WithMockUser
	void getProductInsight_InternalServerError() throws Exception {
		// 서버 오류를 강제하기 위한 잘못된 요청 데이터
		ObjectMapper objectMapper = new ObjectMapper();
		Map<String, Object> requestData = Map.of(
			"articleId", -1L,
			"productId", -1L
		);

		mockMvc.perform(post("/api/anthropic/effect")
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(requestData))
				.header("Authorization", validToken))
			.andDo(print())
			.andExpect(status().isInternalServerError())
			.andExpect(jsonPath("$.code").value(500))
			.andExpect(jsonPath("$.message").value("내부 서버 오류 발생: 예상치 못한 오류"));
	}
}