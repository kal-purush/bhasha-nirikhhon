package com.happiday.Happi_Day.domain.repository.artist;

import com.happiday.Happi_Day.domain.entity.article.Article;
import com.happiday.Happi_Day.domain.entity.artist.Artist;
import com.happiday.Happi_Day.domain.entity.artist.ArtistArticle;
import com.happiday.Happi_Day.domain.entity.user.RoleType;
import com.happiday.Happi_Day.domain.entity.user.User;
import com.happiday.Happi_Day.domain.repository.ArticleRepository;
import com.happiday.Happi_Day.domain.repository.ArtistArticleRepository;
import com.happiday.Happi_Day.domain.repository.ArtistRepository;
import com.happiday.Happi_Day.domain.repository.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
public class ArtistArticleRepositoryTest {

    @Autowired
    private ArtistArticleRepository artistArticleRepository;

    @Autowired
    private ArtistRepository artistRepository;

    @Autowired
    private ArticleRepository articleRepository;

    @Autowired
    private UserRepository userRepository;

    private User user;
    private Artist artist;
    private Article article;
    private ArtistArticle artistArticle;

    @BeforeEach
    public void init() {
        // 유저, 아티스트, 게시글 생성
        user = userRepository.save(User.builder()
                .username("Test Username")
                .password("Test Password")
                .nickname("Test")
                .realname("Test Realname")
                .phone("01012345678")
                .role(RoleType.USER)
                .isActive(true)
                .isTermsAgreed(true)
                .build());
        artist = artistRepository.save(Artist.builder()
                .name("Test Artist")
                .description("Test Artist Description")
                .profileUrl("http://example.com/profile.jpg")
                .build());
        article = articleRepository.save(Article.builder()
                .user(user)
                .title("Test Title")
                .content("Test Content")
                .thumbnailUrl("http://example.com/thumbnail.jpg")
                .build());

        // 아티스트 게시글 관계 설정
        artistArticle = ArtistArticle.builder()
                .article(article)
                .artist(artist)
                .build();
        artistArticleRepository.save(artistArticle);
    }

    @Test
    @DisplayName("게시글과 아티스트로 아티스트 게시글 삭제")
    public void deleteByArticleAndArtistTest() {
        // when
        artistArticleRepository.deleteByArticleAndArtist(article, artist);

        // then
        boolean exists = artistArticleRepository.existsById(artistArticle.getId());
        assertThat(exists).isFalse();
    }

    @Test
    @DisplayName("아티클로 아티스트 게시글 삭제")
    public void deleteByArticleTest() {
        // when
        artistArticleRepository.deleteByArticle(article);

        // then
        boolean exists = artistArticleRepository.existsById(artistArticle.getId());
        assertThat(exists).isFalse();
    }
}