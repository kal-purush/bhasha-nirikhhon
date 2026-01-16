package com.happiday.Happi_Day.domain.repository.team;

import com.happiday.Happi_Day.domain.entity.article.Article;
import com.happiday.Happi_Day.domain.entity.team.Team;
import com.happiday.Happi_Day.domain.entity.team.TeamArticle;
import com.happiday.Happi_Day.domain.entity.user.RoleType;
import com.happiday.Happi_Day.domain.entity.user.User;
import com.happiday.Happi_Day.domain.repository.ArticleRepository;
import com.happiday.Happi_Day.domain.repository.TeamArticleRepository;
import com.happiday.Happi_Day.domain.repository.TeamRepository;
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
public class TeamArticleRepositoryTest {

    @Autowired
    private TeamArticleRepository teamArticleRepository;

    @Autowired
    private ArticleRepository articleRepository;

    @Autowired
    private TeamRepository teamRepository;

    @Autowired
    private UserRepository userRepository;

    private Article article;
    private Team team;
    private TeamArticle teamArticle;
    private User user;

    @BeforeEach
    public void init() {
        // 유저, 팀, 게시글 생성
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
        article = articleRepository.save(Article.builder()
                .user(user)
                .title("Test Title")
                .content("Test Content")
                .thumbnailUrl("http://example.com/thumbnail.jpg")
                .build());
        team = teamRepository.save(Team.builder()
                .name("Test Team")
                .description("Test Description")
                .logoUrl("http://example.com/logo.jpg")
                .build());

        // 팀 게시글 관계 설정
        teamArticle = TeamArticle.builder()
                .team(team)
                .article(article)
                .build();
        teamArticleRepository.save(teamArticle);
    }

    @Test
    @DisplayName("게시글과 팀으로 팀 게시글 삭제")
    public void deleteByArticleAndTeamTest() {
        // when
        teamArticleRepository.deleteByArticleAndTeam(article, team);

        // then
        boolean exists = teamArticleRepository.findById(teamArticle.getId()).isPresent();
        assertThat(exists).isFalse(); // 팀-게시글이 삭제되었는지 확인
    }

    @Test
    @DisplayName("게시글로 팀 게시글 삭제")
    public void deleteByArticleTest() {
        // given - 추가 팀-게시글 데이터 생성
        Team otherTeam = teamRepository.save(
                team = teamRepository.save(Team.builder()
                        .name("Test Team")
                        .description("Test Description")
                        .logoUrl("http://example.com/logo.jpg")
                        .build()));
        TeamArticle anotherTeamArticle = teamArticleRepository.save(
                TeamArticle.builder()
                        .team(otherTeam)
                        .article(article)
                        .build());

        // when
        teamArticleRepository.deleteByArticle(article);

        // then
        boolean existsFirst = teamArticleRepository.findById(teamArticle.getId()).isPresent();
        boolean existsSecond = teamArticleRepository.findById(anotherTeamArticle.getId()).isPresent();
        assertThat(existsFirst).isFalse(); // 첫 번째 팀-게시글이 삭제되었는지 확인
        assertThat(existsSecond).isFalse(); // 두 번째 팀-게시글도 삭제되었는지 확인
    }
}