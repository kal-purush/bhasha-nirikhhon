package com.example.lifeonhana.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
@Slf4j
public class RecommendationClient {
	private final RestTemplate restTemplate;
	private final Random random = new Random();

	@Value("${recommendation.service.url:http://localhost:5001}")
	private String recommendationServiceUrl;

	@Value("${recommendation.service.size:20}")  // size 기본값 20으로 수정
	private int defaultSize;

	public List<Long> getRecommendedArticleIds(Long userId) {
		int randomSeed = random.nextInt(10000);

		String url = UriComponentsBuilder
			.fromUriString(recommendationServiceUrl)
			.path("/api/articles/recommendations")
			.queryParam("userId", userId)
			.queryParam("size", defaultSize)
			.queryParam("seed", randomSeed)
			.build()
			.toUriString();

		log.info("Requesting recommendations with URL: {}", url);

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<String> entity = new HttpEntity<>(headers);

		try {
			ResponseEntity<Map> response = restTemplate.exchange(
				url,
				HttpMethod.GET,
				entity,
				Map.class
			);

			log.info("Recommendation service response status: {}", response.getStatusCode());

			if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
				Map<String, Object> responseBody = response.getBody();
				log.info("Full response body: {}", responseBody);

				Map<String, Object> data = (Map<String, Object>) responseBody.get("data");
				if (data == null) {
					log.error("Data field is null in response");
					throw new RuntimeException("추천 시스템 응답에 data 필드가 없습니다");
				}

				List<?> recommendedArticles = (List<?>) data.get("recommendedArticles");
				if (recommendedArticles == null) {
					log.error("RecommendedArticles field is null in data");
					throw new RuntimeException("추천 시스템 응답에 recommendedArticles 필드가 없습니다");
				}

				// Safer conversion with more detailed error handling
				List<Long> result = recommendedArticles.stream()
					.map(item -> {
						try {
							if (item == null) {
								throw new RuntimeException("Null article ID encountered");
							}
							if (item instanceof Number) {
								return ((Number) item).longValue();
							}
							if (item instanceof String) {
								return Long.parseLong(((String) item).trim());
							}
							throw new RuntimeException("Unexpected type for article ID: " + item.getClass());
						} catch (NumberFormatException e) {
							log.error("Failed to parse article ID: {}", item);
							throw new RuntimeException("Invalid article ID format: " + item);
						}
					})
					.collect(Collectors.toList());

				log.info("Received {} recommended articles: {}", result.size(), result);
				return result;
			} else {
				log.error("Failed response from recommendation service: {}", response);
				throw new RuntimeException("추천 시스템 API 호출 실패");
			}
		} catch (Exception e) {
			log.error("추천 시스템 API 호출 중 오류 발생: {}", e.getMessage(), e);
			throw new RuntimeException("추천 시스템 API 호출 실패: " + e.getMessage());
		}
	}
}