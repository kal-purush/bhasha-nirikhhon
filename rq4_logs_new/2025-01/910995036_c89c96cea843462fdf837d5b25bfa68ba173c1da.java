package com.example.lifeonhana.batch;

import java.util.Set;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.example.lifeonhana.entity.Article;
import com.example.lifeonhana.global.exception.NotFoundException;
import com.example.lifeonhana.repository.ArticleRepository;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class RedisToDatabaseSynchronizer {
	private final RedisTemplate<String, Object> redisTemplate;
	private final ArticleRepository articleRepository;

	@Scheduled(fixedRate = 21600000)
	public void syncLikeCountsToDatabase() {
		Set<Object> changedArticleIds = redisTemplate.opsForSet().members("changedArticles");
		if (changedArticleIds == null || changedArticleIds.isEmpty()) {
			return;
		}

		for (Object articleIdObj : changedArticleIds) {
			Long articleId = Long.parseLong(articleIdObj.toString());
			String articleLikeCountKey = "article:" + articleId + ":likeCount";

			Integer likeCount = (Integer) redisTemplate.opsForValue().get(articleLikeCountKey);

			if (likeCount != null) {
				Article article = articleRepository.findById(articleId)
					.orElseThrow(() -> new NotFoundException("해당 기사를 찾을 수 없습니다."));
				article.setLikeCount(likeCount);
				articleRepository.save(article);
			}
		}

		redisTemplate.delete("changedArticles");
	}
}