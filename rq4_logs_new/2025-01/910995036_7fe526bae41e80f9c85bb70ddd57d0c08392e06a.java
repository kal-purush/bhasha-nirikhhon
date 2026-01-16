package com.example.lifeonhana.controller;

import com.example.lifeonhana.entity.User;
import com.example.lifeonhana.repository.ProductRepository;
import com.example.lifeonhana.repository.UserRepository;
import com.example.lifeonhana.service.JwtService;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(
    properties = "spring.profiles.active=test",
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ActiveProfiles("test")
@Transactional
class ProductLikeControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JwtService jwtService;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private ProductRepository productRepository;

    private String validToken;

    @BeforeEach
    void setUp() {
        // 실제 테스트 데이터로 토큰 생성
        User testUser = userRepository.findById(3L)
            .orElseThrow(() -> new RuntimeException("테스트 사용자가 없습니다."));
        validToken = "Bearer " + jwtService.generateAccessToken(testUser.getAuthId(), testUser.getUserId());
    }

    @Test
    @Order(1)
    @DisplayName("좋아요 토글 - 성공")
    void toggleLike_Success() throws Exception {
        Long productId = productRepository.findAll().get(2).getProductId();

        mockMvc.perform(post("/api/users/" + productId + "/like")
                .header("Authorization", validToken))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.code").value(200))
            .andExpect(jsonPath("$.message").value("좋아요 성공"))
            .andExpect(jsonPath("$.data.isLiked").exists())
            .andDo(print());
    }

    @Test
    @Order(3)
    @DisplayName("좋아요 토글 - 취소")
    void cancelLike_Success() throws Exception {
        Long productId = productRepository.findAll().get(2).getProductId();

        mockMvc.perform(post("/api/users/" + productId + "/like")
                        .header("Authorization", validToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.message").value("좋아요 취소 성공"))
                .andExpect(jsonPath("$.data.isLiked").exists())
                .andDo(print());
    }

    @Test
    @Order(2)
    @DisplayName("좋아요한 상품 목록 조회 - 성공")
    void getLikedProducts_Success() throws Exception {
        mockMvc.perform(get("/api/users/liked/products")
                .header("Authorization", validToken)
                .param("category", "FINANCE"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.code").value(200))
            .andExpect(jsonPath("$.message").value("좋아요한 상품 목록 조회 성공"))
            .andDo(print());
    }

    @Test
    @DisplayName("인증되지 않은 사용자 접근")
    void unauthorized_Access() throws Exception {
        mockMvc.perform(get("/api/users/liked/products"))
            .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("잘못된 토큰으로 접근")
    void invalidToken_Access() throws Exception {
        mockMvc.perform(get("/api/users/liked/products")
                .header("Authorization", "Bearer invalid-token"))
            .andExpect(status().isInternalServerError());
    }

    @Test
    @DisplayName("존재하지 않는 상품 좋아요")
    void toggleLike_ProductNotFound() throws Exception {
        mockMvc.perform(post("/api/users/999/like")
                .header("Authorization", validToken))
            .andExpect(status().isBadRequest());
    }

}