package com.example.lifeonhana.controller;

import com.example.lifeonhana.entity.Article;
import com.example.lifeonhana.entity.User;
import com.example.lifeonhana.repository.ArticleRepository;
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
class ArticleLikeControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JwtService jwtService;

    @Autowired
    private ArticleRepository articleRepository;

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
    @Order(1)
    @DisplayName("좋아요 토글 - 성공")
    void toggleLike_Success() throws Exception {
        Article article = articleRepository.findById(15L)
            .orElseThrow(() -> new RuntimeException("테스트 기사가 없습니다."));

        mockMvc.perform(post("/api/articles/" + article.getArticleId() + "/like")
                .header("Authorization", validToken))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.code").value(200))
            .andExpect(jsonPath("$.message").value("좋아요 성공"))
            .andExpect(jsonPath("$.data.isLiked").exists())
            .andExpect(jsonPath("$.data.likeCount").exists())
            .andDo(print());
    }

    @Test
    @Order(2)
    @DisplayName("좋아요 정보 조회 - 성공")
    void getLikeInfo_Success() throws Exception {
        Article article = articleRepository.findById(15L)
            .orElseThrow(() -> new RuntimeException("테스트 기사가 없습니다."));

        mockMvc.perform(get("/api/articles/" + article.getArticleId() + "/like")
                .header("Authorization", validToken))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.code").value(200))
            .andExpect(jsonPath("$.message").value("게시글 좋아요 정보 조회 성공"))
            .andExpect(jsonPath("$.data.isLiked").exists())
            .andExpect(jsonPath("$.data.likeCount").exists())
            .andDo(print());
    }

    @Test
    @Order(3)
    @DisplayName("좋아요한 기사 목록 조회 - 성공")
    void getLikedArticles_Success() throws Exception {
        mockMvc.perform(get("/api/articles/liked")
                .header("Authorization", validToken)
                .param("category", "REAL_ESTATE"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.code").value(200))
            .andExpect(jsonPath("$.message").value("좋아요한 기사 목록 조회 성공"))
            .andDo(print());
    }

    @Test
    @DisplayName("인증되지 않은 사용자 접근")
    void unauthorized_Access() throws Exception {
        mockMvc.perform(get("/api/articles/liked"))
            .andExpect(status().isInternalServerError());
    }

    @Test
    @DisplayName("잘못된 토큰으로 접근")
    void invalidToken_Access() throws Exception {
        mockMvc.perform(get("/api/articles/liked")
                .header("Authorization", "Bearer invalid-token"))
            .andExpect(status().isInternalServerError());
    }

    @Test
    @DisplayName("카테고리 파라미터 검증")
    void validateCategoryParameter() throws Exception {
        mockMvc.perform(get("/api/articles/liked")
                .header("Authorization", validToken)
                .param("category", "INVALID_CATEGORY"))
            .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("페이지네이션 파라미터 검증")
    void validatePaginationParameters() throws Exception {
        mockMvc.perform(get("/api/articles/liked")
                .header("Authorization", validToken)
                .param("page", "-1")
                .param("size", "0"))
            .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("존재하지 않는 기사 좋아요")
    void toggleLike_ArticleNotFound() throws Exception {
        mockMvc.perform(post("/api/articles/999/like")
                .header("Authorization", validToken))
            .andExpect(status().isNotFound());
    }
}