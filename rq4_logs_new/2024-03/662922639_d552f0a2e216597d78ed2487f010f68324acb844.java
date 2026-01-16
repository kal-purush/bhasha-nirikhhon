package com.happiday.Happi_Day.domain.repository.article;

import com.happiday.Happi_Day.domain.entity.article.Article;
import com.happiday.Happi_Day.domain.entity.article.ArticleComment;
import com.happiday.Happi_Day.domain.entity.board.BoardCategory;
import com.happiday.Happi_Day.domain.entity.user.RoleType;
import com.happiday.Happi_Day.domain.entity.user.User;
import com.happiday.Happi_Day.domain.repository.ArticleCommentRepository;
import com.happiday.Happi_Day.domain.repository.ArticleRepository;
import com.happiday.Happi_Day.domain.repository.BoardCategoryRepository;
import com.happiday.Happi_Day.domain.repository.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
public class ArticleCommentRepositoryTest {
    @Autowired
    ArticleRepository articleRepository;

    @Autowired
    BoardCategoryRepository boardCategoryRepository;

    @Autowired
    UserRepository userRepository;

    @Autowired
    ArticleCommentRepository articleCommentRepository;

    private Article article;

    @BeforeEach
    void beforeEach() {
        // 카테고리 생성
        BoardCategory testCategory = BoardCategory.builder().name("test Category").description("test Category description").build();
        boardCategoryRepository.save(testCategory);

        // 유저 생성
        User testUser = User.builder()
                .username("test email")
                .password("test password")
                .nickname("test nickname")
                .realname("test name")
                .phone("01012345678")
                .role(RoleType.USER)
                .isActive(true)
                .isTermsAgreed(true)
                .build();
        testUser = userRepository.save(testUser);

        // 이미지 url 리스트
        List<String> imageList = new ArrayList<>();

        imageList.add("http://example.com/article_image1.jpg");
        imageList.add("http://example.com/article_image2.jpg");
        imageList.add("http://example.com/article_image3.jpg");

        article = Article.builder()
                .user(testUser)
                .category(testCategory)
                .title("test title")
                .content("test content")
                .thumbnailUrl("http://example.com/article_thumnail.jpg")
                .imageUrl(imageList)
                .eventAddress("test eventAddress")
                .build();
        article = articleRepository.save(article);

        ArticleComment comment1 = ArticleComment.builder()
                .user(testUser)
                .article(article)
                .content("test comment1")
                .build();
        ArticleComment comment2 = ArticleComment.builder()
                .user(testUser)
                .article(article)
                .content("test comment2")
                .build();
        articleCommentRepository.save(comment1);
        articleCommentRepository.save(comment2);
    }

    @Test
    @DisplayName("게시글의 댓글 조회(페이징)")
    public void findCommentByArticleWithPaginationTest() {
        // given
        String testComment1 = "test comment1";
        String testComment2 = "test comment2";
        Pageable pageable = PageRequest.of(0, 5);

        // when
        Page<ArticleComment> comments = articleCommentRepository.findAllByArticle(article, pageable);

        // then
        assertThat(comments.getTotalElements()).isEqualTo(2);
        assertThat(comments.getContent().get(0).getContent()).isEqualTo(testComment1);
        assertThat(comments.getContent().get(1).getContent()).isEqualTo(testComment2);
    }

    @Test
    @DisplayName("게시글의 댓글 조회")
    public void findCommentByArticleTest() {
        // given
        String testComment1 = "test comment1";
        String testComment2 = "test comment2";

        // when
        List<ArticleComment> comments = articleCommentRepository.findAllByArticle(article);

        // then
        assertThat(comments).hasSize(2);
        assertThat(comments.get(0).getContent()).isEqualTo(testComment1);
        assertThat(comments.get(1).getContent()).isEqualTo(testComment2);
    }

    @Test
    @DisplayName("유저의 댓글 조회")
    public void findCommentByUserTest() {
        // given
        User testUser = userRepository.findById(article.getUser().getId()).get();
        String testComment1 = "test comment1";
        String testComment2 = "test comment2";
        Pageable pageable = PageRequest.of(0, 5);

        // when
        Page<ArticleComment> comments = articleCommentRepository.findAllByUser(testUser, pageable);

        // then
        assertThat(comments.getTotalElements()).isEqualTo(2);
        assertThat(comments.getContent().get(0).getContent()).isEqualTo(testComment1);
        assertThat(comments.getContent().get(1).getContent()).isEqualTo(testComment2);
    }

}