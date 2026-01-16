package com.gitprism.GitPRism.portfolios.search;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.gitprism.GitPRism.portfolios.document.PortfolioDocument;
import com.gitprism.GitPRism.portfolios.entity.Portfolio;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PortfolioSearchIndexer {

  private final ElasticsearchClient elasticsearchClient;

  public void index(Portfolio portfolio) {
    try {
      PortfolioDocument doc = PortfolioDocument.builder()
          .id(portfolio.getId())
          .title(portfolio.getTitle())
          .description(portfolio.getDescription())
          .createdAt(portfolio.getCreatedAt())
          .updatedAt(portfolio.getUpdatedAt())
          .build();

      elasticsearchClient.index(i -> i
          .index("portfolios")
          .id(doc.getId().toString())
          .document(doc)
      );

      log.info("🔄 Elasticsearch 색인 완료: {}", doc.getTitle());

    } catch (Exception e) {
      log.error("❌ Elasticsearch 색인 실패 (portfolioId={}): {}", portfolio.getId(), e.getMessage());
    }
  }
}