package com.example.lifeonhana.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


import com.example.lifeonhana.entity.User;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import com.example.lifeonhana.service.JwtService;
import com.example.lifeonhana.entity.Article;
import com.example.lifeonhana.repository.ArticleRepository;
import com.example.lifeonhana.repository.UserRepository;
import org.mockito.Mockito;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.AuthenticationException;

@SpringBootTest(
    properties = "spring.profiles.active=test",
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ActiveProfiles("test")
@Transactional
class ArticleControllerTest {

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
    @DisplayName("기사 상세 조회 - 성공")
    void getArticleDetails_Success() throws Exception {
        Long articleId = 15L;  // 실제 DB에 있는 게시글 ID

        mockMvc.perform(get("/api/articles/{articleId}", articleId)
                .header("Authorization", validToken))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.code").value(200))
            .andExpect(jsonPath("$.status").value("OK"))
            .andExpect(jsonPath("$.message").value("기사 상세 조회 성공"))
            .andExpect(jsonPath("$.data.articleId").value(articleId))
            .andExpect(jsonPath("$.data.category").value("REAL_ESTATE"));
    }

    @Test
    @Order(2)
    @DisplayName("기사 목록 조회 - 성공")
    void getArticles_Success() throws Exception {
        mockMvc.perform(get("/api/articles")
                .param("category", "REAL_ESTATE")
                .param("page", "1")
                .param("size", "10")
                .header("Authorization", validToken))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.code").value(200))
            .andExpect(jsonPath("$.message").value("기사 목록 조회 성공"))
            .andExpect(jsonPath("$.data.articles[0].title").value("은퇴자의 효율적인 주택 다운사이징 방안"))
            .andDo(print());
    }

    @Test
    @Order(3)
    @DisplayName("기사 검색 - 성공")
    void searchArticles_Success() throws Exception {
        mockMvc.perform(get("/api/articles/search")
                .param("query", "여행")
                .param("page", "1")
                .param("size", "5")
                .header("Authorization", validToken))
            .andExpect(status().isOk())
            .andDo(print());
    }

    @Test
    @DisplayName("존재하지 않는 기사 조회")
    void getArticleDetails_NotFound() throws Exception {
        Long nonExistentArticleId = 999L;

        mockMvc.perform(get("/api/articles/{articleId}", nonExistentArticleId)
                .header("Authorization", validToken))
            .andExpect(jsonPath("$.code").value(400))
            .andExpect(jsonPath("$.status").value("BAD_REQUEST"))
            .andExpect(jsonPath("$.message").value("존재하지 않는 ID입니다."));
    }

    @Test
    @DisplayName("서버 내부 오류")
    void getArticleDetails_InternalServerError() throws Exception {
        Long articleId = -1L; // 서버 오류를 발생시키는 ID라고 가정

        mockMvc.perform(get("/api/articles/{id}", articleId)
                        .header("Authorization", validToken))
                .andExpect(jsonPath("$.code").value(400));
    }

    @Test
    @DisplayName("잘못된 카테고리로 기사 목록 조회")
    void getArticles_InvalidCategory() throws Exception {
        mockMvc.perform(get("/api/articles")
                .param("category", "INVALID_CATEGORY")
                .header("Authorization", validToken))
            .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("인증되지 않은 사용자 접근")
    void unauthorizedAccess() throws Exception {
        mockMvc.perform(get("/api/articles/1"))
                .andExpect(jsonPath("$.code").value(404));
    }

    @Test
    @DisplayName("검색어 최소 길이 검증")
    void searchQuery_MinLength() throws Exception {
        mockMvc.perform(get("/api/articles/search")
                .param("query", "a")
                .header("Authorization", validToken))
            .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("검색어 최대 길이 검증")
    void searchQuery_MaxLength() throws Exception {
        String longQuery = "a".repeat(101);
        mockMvc.perform(get("/api/articles/search")
                .param("query", longQuery)
                .header("Authorization", validToken))
            .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("검색어 없이 검색 요청")
        void searchArticles_WithoutQuery() throws Exception {
            mockMvc.perform(get("/api/articles/search")
            .header("Authorization", validToken))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.code").value(200))
        .andExpect(jsonPath("$.data.articles").exists())
        .andExpect(jsonPath("$.data.hasNext").exists());
    }
        @Test
        @DisplayName("정상적인 검색 요청")
        void searchArticles_ValidQuery() throws Exception {
            mockMvc.perform(get("/api/articles/search")
                  .param("query", "테스트")
                 .header("Authorization", validToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.message").value("기사 검색 성공"));
    }




    @Test
    @DisplayName("잘못된 토큰으로 기사 목록 조회")
    void getArticles_InvalidToken() throws Exception {
        // when & then
        mockMvc.perform(get("/api/articles")
                .header("Authorization", "invalid_token"))
            .andExpect(jsonPath("$.code").value(500))
            .andExpect(jsonPath("$.message").value("기사 목록 조회 중 오류가 발생했습니다."));
    }

    @Test
    @DisplayName("토큰 없이 기사 목록 조회")
    void getArticles_NoToken() throws Exception {
        // when & then
        mockMvc.perform(get("/api/articles"))
            .andExpect(jsonPath("$.code").value(500))
            .andExpect(jsonPath("$.status").value("INTERNAL_SERVER_ERROR"))
            .andExpect(jsonPath("$.message").value("기사 목록 조회 중 오류가 발생했습니다."));
    }
} 
