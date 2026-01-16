package com.happiday.Happi_Day.domain.service;

import com.happiday.Happi_Day.domain.entity.article.Article;
import com.happiday.Happi_Day.domain.entity.article.ArticleLike;
import com.happiday.Happi_Day.domain.entity.article.dto.ReadListArticleDto;
import com.happiday.Happi_Day.domain.entity.article.dto.WriteArticleDto;
import com.happiday.Happi_Day.domain.entity.artist.Artist;
import com.happiday.Happi_Day.domain.entity.artist.ArtistArticle;
import com.happiday.Happi_Day.domain.entity.artist.ArtistSubscription;
import com.happiday.Happi_Day.domain.entity.board.BoardCategory;
import com.happiday.Happi_Day.domain.entity.user.RoleType;
import com.happiday.Happi_Day.domain.entity.user.User;
import com.happiday.Happi_Day.domain.repository.*;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.AfterEach;
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
public class ArticleServiceTest {
    @Autowired
    BoardCategoryRepository boardCategoryRepository;

    @Autowired
    UserRepository userRepository;

    @Autowired
    HashtagRepository hashtagRepository;

    @Autowired
    ArticleService articleService;

    @Autowired
    ArticleRepository articleRepository;

    @Autowired
    ArtistRepository artistRepository;

    @Autowired
    ArtistArticleRepository artistArticleRepository;

    @Autowired
    ArtistSubscriptionRepository artistSubscriptionRepository;

    @Autowired
    ArticleLikeRepository articleLikeRepository;

    @Autowired
    private EntityManager entityManager;

    private BoardCategory testCategory;
    private User testUser;
    private MultipartFile thumbnailImage;
    private List<MultipartFile> imageList;
    private Article testArticle;
    private Artist artist;

    @AfterEach
    void afterEach() {
        articleRepository.deleteAll();
    }

    @BeforeEach
    void beforeEach() {
        // 카테고리 생성
        testCategory = BoardCategory.builder().name("test Category").description("test Category description").build();
        boardCategoryRepository.save(testCategory);

        // 아티스트 생성
        List<Artist> artists = new ArrayList<>();
        artist = Artist.builder()
                .name("artist")
                .description("artist description")
                .profileUrl("imageUrl")
                .build();
        artistRepository.save(artist);
        artists.add(artist);

        // 구독한 아티스트
        List<ArtistSubscription> subList = new ArrayList<>();
        ArtistSubscription sub = ArtistSubscription.builder()
                .user(testUser)
                .artist(artist)
                .build();
        artistSubscriptionRepository.save(sub);
        subList.add(sub);

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
                .artistSubscriptionList(subList)
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

    }

    @Test
    @DisplayName("게시글 작성")
    public void createArticleTest() {
        // given
        List<String> hashtags = new ArrayList<>();
        hashtags.add("hashtag1");
        hashtags.add("hashtag2");

        WriteArticleDto dto = WriteArticleDto.builder()
                .title("test Article title")
                .content("test Article content")
                .eventAddress("test address")
                .eventDetailAddress("test detailAddress")
                .hashtag(hashtags)
                .build();

        // when
        articleService.writeArticle(testCategory.getId(), dto, thumbnailImage, imageList, testUser.getUsername());

        // then
        articleRepository.findById(2L).ifPresent(article -> {
            assertThat(article.getTitle()).isEqualTo("test Article title");
        });
    }

    @Test
    @DisplayName("게시글 상세 조회")
    public void readOneArticleTest() {
        // given
        String clientAddress = "192.168.0.1";

        // when
        articleService.readOne(clientAddress, testArticle.getId());

        // then
        articleRepository.findById(1L).ifPresent(article -> {
            assertThat(article.getTitle()).isEqualTo("Article title");
            assertThat(article.getContent()).isEqualTo("Article content");
        });
    }

    @Test
    @DisplayName("게시글 목록 조회 / 전체글 조회")
    public void readListArticleByCategoryTest() {
        // given
        testArticle = Article.builder()
                .user(testUser)
                .category(testCategory)
                .title("test Article title")
                .content("test Article content")
                .thumbnailUrl(thumbnailImage.toString())
                .imageUrl(imageList.stream().map(image -> image.toString()).collect(Collectors.toList()))
                .articleComments(new ArrayList<>())
                .artistArticleList(new ArrayList<>())
                .teamArticleList(new ArrayList<>())
                .articleHashtags(new ArrayList<>())
                .articleLikes(new ArrayList<>())
                .build();
        articleRepository.save(testArticle);

        Pageable pageable = PageRequest.of(0, 12);
        String filter = "name";
        String keyword = "test";

        // when
        Page<ReadListArticleDto> readListArticleWithFilter = articleService.readList(testCategory.getId(), pageable, filter, keyword);
        Page<ReadListArticleDto> readListArticle = articleService.readList(testCategory.getId(), pageable, null, null);

        Page<ReadListArticleDto> readAllListArticle = articleService.readList(pageable, null, null);

        // then
        assertThat(readListArticleWithFilter).isNotNull();
        assertThat(readListArticleWithFilter.getContent().size()).isEqualTo(1);

        assertThat(readListArticle).isNotNull();
        assertThat(readListArticle.getContent().size()).isEqualTo(2);

        assertThat(readAllListArticle).isNotNull();
        assertThat(readAllListArticle.getContent().size()).isEqualTo(2);
    }

    @Test
    @DisplayName("구독중인 아티스트 글 조회")
    public void readListArticleBySubscribedArtistsTest() {
        // given
        testArticle = Article.builder()
                .user(testUser)
                .category(testCategory)
                .title("test Article title")
                .content("test Article content")
                .thumbnailUrl(thumbnailImage.toString())
                .imageUrl(imageList.stream().map(image -> image.toString()).collect(Collectors.toList()))
                .articleComments(new ArrayList<>())
                .artistArticleList(new ArrayList<>())
                .teamArticleList(new ArrayList<>())
                .articleHashtags(new ArrayList<>())
                .articleLikes(new ArrayList<>())
                .build();
        articleRepository.save(testArticle);

        List<ArtistArticle> artistArticleList = new ArrayList<>();
        ArtistArticle artistArticle = ArtistArticle.builder()
                .article(testArticle)
                .artist(artist)
                .build();
        artistArticleRepository.save(artistArticle);
        artistArticleList.add(artistArticle);

        Pageable pageable = PageRequest.of(0, 12);
        String filter = "name";
        String keyword = "test";

        // when
        Page<ReadListArticleDto> articleList = articleService.readArticleBySubscribedArtists(pageable, testCategory.getId(), filter, keyword, testUser.getUsername());

        // then
        System.out.println(testUser.getArtistSubscriptionList());

        assertThat(articleList).isNotNull();
        assertThat(articleList.getContent().size()).isEqualTo(1);
    }

    @Test
    @DisplayName("게시글 수정")
    public void updateArticleTest() {
        // given
        List<String> hashtags = new ArrayList<>();
        hashtags.add("update hashtag1");
        hashtags.add("update hashtag2");

        WriteArticleDto dto = WriteArticleDto.builder()
                .title("update Article title")
                .content("update Article content")
                .eventAddress("update address")
                .eventDetailAddress("update detailAddress")
                .hashtag(hashtags)
                .build();

        // when
        articleService.updateArticle(testArticle.getId(), dto, testUser.getUsername(), thumbnailImage, imageList);

        // then
        articleRepository.findById(1L).ifPresent(article -> {
            assertThat(article.getTitle()).isEqualTo("update Article title");
            assertThat(article.getContent()).isEqualTo("update Article content");
        });
    }

    @Test
    @DisplayName("게시글 삭제")
    public void deleteArticleTest() {
        // when
        articleService.deleteArticle(testArticle.getId(), testUser.getUsername());

        // then
        List<Article> articleList = articleRepository.findAll();
        assertThat(articleList).isNotNull();
        assertThat(articleList.size()).isEqualTo(0);
    }

    @Test
    @DisplayName("게시글 좋아요")
    public void likeArticleTest() {
        // when
        articleService.likeArticle(testArticle.getId(), testUser.getUsername());

        // then
        List<ArticleLike> articleLikes = articleLikeRepository.findByArticle(testArticle);
        assertThat(articleLikes).isNotNull();
        assertThat(articleLikes.size()).isEqualTo(1);
    }

    @Test
    @DisplayName("게시글 조회수 증가")
    public void increaseViewCountTest() {
        // given
        String clientAddress = "192.168.0.1";
        int beforeIncrease = testArticle.getViewCount();

        // when
        articleService.increaseViewCount(clientAddress, testArticle.getId());
        entityManager.flush();
        entityManager.clear();

        // then
        Article updateArticle = articleRepository.findById(testArticle.getId()).get();
        assertThat(updateArticle.getViewCount()).isEqualTo(beforeIncrease + 1);
    }
}