package com.gitprism.GitPRism.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig {

  @Bean
  public ElasticsearchClient elasticsearchClient() {
    // ✅ 기존 JacksonJsonpMapper 대신 커스텀 ObjectMapper 생성
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule()); // LocalDateTime 직렬화 가능

    JacksonJsonpMapper jsonpMapper = new JacksonJsonpMapper(mapper);

    RestClient restClient = RestClient.builder(
        new HttpHost(
            System.getenv("ELASTICSEARCH_HOST"),
            Integer.parseInt(System.getenv("ELASTICSEARCH_PORT")),
            "http"
        )
    ).build();

    return new ElasticsearchClient(new RestClientTransport(restClient, jsonpMapper));
  }
}