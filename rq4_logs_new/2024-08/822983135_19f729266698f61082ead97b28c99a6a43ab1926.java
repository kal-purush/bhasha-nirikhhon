package org.hhplus.hhplus_concert_service.queueTest;

import org.hhplus.hhplus_concert_service.business.service.TokenQueueService;
import org.hhplus.hhplus_concert_service.domain.TokenQueue;
import org.hhplus.hhplus_concert_service.persistence.TokenQueueRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class queueTest {
    @Autowired
    private TokenQueueService tokenQueueService;

    @Autowired
    private RedisTemplate<String, TokenQueue> redisTemplate;

    @Autowired
    private TokenQueueRepository tokenQueueRepository;

    @BeforeEach
    public void setUp() {
        // 테스트 전에 필요한 초기화 작업을 수행합니다.
        redisTemplate.getConnectionFactory().getConnection().flushAll(); // Redis 초기화
        tokenQueueRepository.deleteAll(); // DB 초기화
    }

    @Test
    public void testAddTokenQueue() {
        // given
        String userId = "user1";
        int concertId = 123;

        // when
        tokenQueueService.addTokenQueue(userId, concertId);

        // then
        List<TokenQueue> tokens = tokenQueueService.getAllTokens(concertId);
        assertThat(tokens).hasSize(1);
        assertThat(tokens.get(0).getUserId()).isEqualTo(userId);
    }

    @Test
    public void testActivateTokens() {
        // given
        String userId = "user2";
        int concertId = 456;
        tokenQueueService.addTokenQueue(userId, concertId);

        // when
        tokenQueueService.activateTokens(concertId);

        // then
        List<TokenQueue> tokens = tokenQueueService.getAllTokens(concertId);
        assertThat(tokens).hasSize(1);
        assertThat(tokens.get(0).isActive()).isTrue();
    }

    @Test
    public void testIsTokenValid() {
        // given
        String userId = "user3";
        int concertId = 789;
        tokenQueueService.addTokenQueue(userId, concertId);
        List<TokenQueue> tokens = tokenQueueService.getAllTokens(concertId);
        String token = tokens.get(0).getToken();

        // when
        boolean isValid = tokenQueueService.isTokenValid(concertId, token);

        // then
        assertThat(isValid).isTrue();
    }
}