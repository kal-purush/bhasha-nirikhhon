package com.github.ssullivan;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.jupiter.api.Test;

public class RedisConfigJacksonTest {
  private static final ObjectMapper ObjectMapper = new ObjectMapper();

  @Test
  public void testReading() throws IOException {
    final String json = "{\"host\":\"test\", \"port\":1234, \"timeout\":10, \"db\": 1}";
    final RedisConfig redisConfig = ObjectMapper.readValue(json, RedisConfig.class);

    MatcherAssert.assertThat(redisConfig.getHost(), Matchers.equalTo("test"));
    MatcherAssert.assertThat(redisConfig.getPort(), Matchers.equalTo(1234));
    MatcherAssert.assertThat(redisConfig.getTimeout(), Matchers.equalTo(10L));
    MatcherAssert.assertThat(redisConfig.getDb(), Matchers.equalTo(1));
  }

  @Test
  public void testSerialization() throws IOException {
    final RedisConfig redisConfig = new RedisConfig("localhost", 6379);
    final String json = ObjectMapper.writeValueAsString(redisConfig);
    final RedisConfig fromJson = ObjectMapper.readValue(json, RedisConfig.class);
    MatcherAssert.assertThat(fromJson.getHost(), Matchers.equalTo("localhost"));
    MatcherAssert.assertThat(fromJson.getPort(), Matchers.equalTo(6379));
  }
}