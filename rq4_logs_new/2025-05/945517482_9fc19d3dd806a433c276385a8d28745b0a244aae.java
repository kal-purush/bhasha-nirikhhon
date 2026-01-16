package com.gitprism.GitPRism.portfolios.service;

import com.gitprism.GitPRism.portfolios.entity.Portfolio;
import com.gitprism.GitPRism.portfolios.repository.PortfolioRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class PortfolioRankingInitializer {

  private final RedisTemplate<String, String> redisTemplate;
  private final PortfolioRepository portfolioRepository;

  private static final String POPULAR_KEY = "popular_portfolios";

  @EventListener(ContextRefreshedEvent.class)
  public void restoreRanking() {
    log.info("Redis 인기 포트폴리오 복원 시작...");

    // ✅ 올바른 enum 타입 사용
    List<Portfolio> published = portfolioRepository.findAllByStatusAndIsDeletedFalse(Portfolio.Status.PUBLISHED);

    for (Portfolio portfolio : published) {
      String member = String.valueOf(portfolio.getId());
      redisTemplate.opsForZSet().add(POPULAR_KEY, member, 0.0);
    }

    log.info("✅ Redis 복원 완료! 총 {}개 포트폴리오", published.size());
  }
}