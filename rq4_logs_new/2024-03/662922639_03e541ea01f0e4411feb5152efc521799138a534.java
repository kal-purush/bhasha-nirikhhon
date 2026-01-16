package com.happiday.Happi_Day.domain.service.article;

import com.happiday.Happi_Day.domain.entity.article.Article;
import com.happiday.Happi_Day.domain.entity.article.ArticleComment;
import com.happiday.Happi_Day.domain.entity.article.dto.WriteCommentDto;
import com.happiday.Happi_Day.domain.entity.board.BoardCategory;
import com.happiday.Happi_Day.domain.entity.user.RoleType;
import com.happiday.Happi_Day.domain.entity.user.User;
import com.happiday.Happi_Day.domain.repository.*;
import com.happiday.Happi_Day.domain.service.ArticleCommentService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
public class ArticleCommentServiceTest {
    @Autowired
    BoardCategoryRepository boardCategoryRepository;

    @Autowired
    UserRepository userRepository;

    @Autowired
    ArticleRepository articleRepository;

    @Autowired
    ArticleCommentService articleCommentService;

    @Autowired
    ArticleCommentRepository articleCommentRepository;


    private User testUser;
    private BoardCategory testCategory;
    private MultipartFile thumbnailImage;
    private List<MultipartFile> imageList;
    private Article testArticle;
    private ArticleComment testComment;

    @BeforeEach
    void beforeEach() {
        // 카테고리 생성
        testCategory = BoardCategory.builder().name("test Category").description("test Category description").build();
        boardCategoryRepository.save(testCategory);

        // 유저 생성
        testUser = User.builder()
                .username("test email")
                .password("test password")
                .nickname("test nickname")
                .realname("test name")
                .phone("01012345678")
                .role(RoleType.USER)
                .isActive(true)
                .isTermsAgreed(true)
                .teamSubscriptionList(new ArrayList<>())
                .build();
        testUser = userRepository.save(testUser);

        // 썸네일 이미지 생성
        thumbnailImage = new MockMultipartFile("thumbnailImage", "thumbnailImage.jpg", MediaType.IMAGE_JPEG_VALUE, "ImageData".getBytes());

        // 이미지 생성
        imageList = new ArrayList<>();
        MultipartFile image1 = new MockMultipartFile("image1", "image1.jpg", MediaType.IMAGE_JPEG_VALUE, "ImageData".getBytes());
        MultipartFile image2 = new MockMultipartFile("image2", "image2.jpg", MediaType.IMAGE_JPEG_VALUE, "ImageData".getBytes());
        MultipartFile image3 = new MockMultipartFile("image3", "image3.jpg", MediaType.IMAGE_JPEG_VALUE, "ImageData".getBytes());
        imageList.add(image1);
        imageList.add(image2);
        imageList.add(image3);

        // 글 생성
        testArticle = Article.builder()
                .user(testUser)
                .category(testCategory)
                .title("Article title")
                .content("Article content")
                .thumbnailUrl(thumbnailImage.toString())
                .imageUrl(imageList.stream().map(image -> image.toString()).collect(Collectors.toList()))
                .articleComments(new ArrayList<>())
                .artistArticleList(new ArrayList<>())
                .teamArticleList(new ArrayList<>())
                .articleHashtags(new ArrayList<>())
                .articleLikes(new ArrayList<>())
                .build();
        articleRepository.save(testArticle);

        testComment = ArticleComment.builder()
                .article(testArticle)
                .user(testUser)
                .content("comment")
                .build();
        articleCommentRepository.save(testComment);


    }

    @Test
    @DisplayName("게시글 댓글 작성")
    public void writeCommentTest() {
        // given
        WriteCommentDto dto = new WriteCommentDto();
        dto.setContent("test comment");

        // when
        articleCommentService.writeComment(testArticle.getId(), dto, testUser.getUsername());

        // then
        List<ArticleComment> commentList = articleCommentRepository.findAllByArticle(testArticle);
        assertThat(commentList.size()).isEqualTo(2);
        assertThat(commentList.get(1).getContent()).isEqualTo("test comment");
    }

    @Test
    @DisplayName("게시글 댓글 조회")
    public void readCommentTest() {
        // given
        Pageable pageable = PageRequest.of(0, 5);

        // when
        articleCommentService.readComment(testArticle.getId(), pageable);

        // then
        Page<ArticleComment> comments = articleCommentRepository.findAllByArticle(testArticle, pageable);
        assertThat(comments.getContent().size()).isNotNull();
        assertThat(comments.getContent().size()).isEqualTo(1);
    }

    @Test
    @DisplayName("게시글 댓글 수정")
    public void updateCommentTest() {
        // given
        WriteCommentDto dto = new WriteCommentDto();
        dto.setContent("update comment");

        // when
        articleCommentService.updateComment(testArticle.getId(), testComment.getId(), dto);

        // then
        List<ArticleComment> commentList = articleCommentRepository.findAllByArticle(testArticle);
        assertThat(commentList.get(0).getContent()).isEqualTo("update comment");
    }

    @Test
    @DisplayName("게시글 댓글 삭제")
    public void deleteCommentTest() {
        // when
        articleCommentService.deleteComment(testArticle.getId(), testComment.getId());

        // then
        List<ArticleComment> commentList = articleCommentRepository.findAllByArticle(testArticle);
        assertThat(commentList.size()).isEqualTo(0);
    }
}