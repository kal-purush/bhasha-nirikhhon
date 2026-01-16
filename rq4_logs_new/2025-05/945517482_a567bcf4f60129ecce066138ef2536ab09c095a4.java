package com.gitprism.GitPRism.config;

import com.gitprism.GitPRism.github_users.entity.GitHubUser;
import com.gitprism.GitPRism.github_users.repository.GitHubUserRepository;
import com.gitprism.GitPRism.portfolios.entity.Portfolio;
import com.gitprism.GitPRism.portfolios.repository.PortfolioRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Component
@RequiredArgsConstructor
public class MockDataLoader implements CommandLineRunner {

    private final GitHubUserRepository gitHubUserRepository;
    private final PortfolioRepository portfolioRepository;

    @Override
    public void run(String... args) {
        if (portfolioRepository.count() > 0) return;

        GitHubUser user1 = gitHubUserRepository.findById(1L).orElseGet(() -> gitHubUserRepository.save(
                GitHubUser.builder()
                        .id(1L)
                        .githubId("12345678")
                        .username("hong_dev")
                        .email("hong_dev@example.com")
                        .accessToken("ghp_mockToken1")
                        .createdAt(LocalDateTime.now())
                        .updatedAt(LocalDateTime.now())
                        .deleted(false)
                        .build()
        ));

        GitHubUser user2 = gitHubUserRepository.findById(2L).orElseGet(() -> gitHubUserRepository.save(
                GitHubUser.builder()
                        .id(2L)
                        .githubId("23456789")
                        .username("yeji_log")
                        .email("yeji_log@example.com")
                        .accessToken("ghp_mockToken2")
                        .createdAt(LocalDateTime.now())
                        .updatedAt(LocalDateTime.now())
                        .deleted(false)
                        .build()
        ));

        GitHubUser user3 = gitHubUserRepository.findById(3L).orElseGet(() -> gitHubUserRepository.save(
                GitHubUser.builder()
                        .id(3L)
                        .githubId("34567890")
                        .username("james_code")
                        .email("james_code@example.com")
                        .accessToken("ghp_mockToken3")
                        .createdAt(LocalDateTime.now())
                        .updatedAt(LocalDateTime.now())
                        .deleted(false)
                        .build()
        ));

        Portfolio p1 = Portfolio.builder()
                .user(user1)
                .title("Git 포트폴리오 관리 공유 프로젝트")
                .description("이 프로젝트는...")
                .status(Portfolio.Status.PUBLISHED)
                .isDeleted(false)
                .createdAt(LocalDateTime.of(2025, 4, 10, 14, 12))
                .updatedAt(LocalDateTime.of(2025, 4, 10, 14, 12))
                .build();

        Portfolio p2 = Portfolio.builder()
                .user(user2)
                .title("개발자 일일 회고 자동 기록 서비스")
                .description("매일 자동으로 회고...")
                .status(Portfolio.Status.PUBLISHED)
                .isDeleted(false)
                .createdAt(LocalDateTime.of(2025, 4, 9, 10, 30))
                .updatedAt(LocalDateTime.of(2025, 4, 9, 10, 30))
                .build();

        Portfolio p3 = Portfolio.builder()
                .user(user3)
                .title("PR 분석 기반 코드 리뷰 피드백 시스템")
                .description("OpenAI를 활용한...")
                .status(Portfolio.Status.PUBLISHED)
                .isDeleted(false)
                .createdAt(LocalDateTime.of(2025, 4, 8, 16, 45))
                .updatedAt(LocalDateTime.of(2025, 4, 8, 16, 45))
                .build();

        portfolioRepository.saveAll(List.of(p1, p2, p3));

        System.out.println("목업 유저 및 포트폴리오 생성 완료!");
    }
}