/*
 * Copyright (c) 2019.
 * This file is part of project ***
 * Written by Sergiy Krasnikov <sergei29041996@gmail.com>
 */

package com.controller;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.json.JsonToObjectTransformer;
import org.springframework.integration.support.json.Jackson2JsonObjectMapper;
import org.springframework.integration.support.json.JsonObjectMapper;

/**
 * Created by User on 17.03.2019.
 */
@Configuration
public class IntegrationConfiguration {

  @Bean
  public JsonObjectMapper<JsonNode, JsonParser> jsonObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    return new Jackson2JsonObjectMapper(mapper);
  }

  @Bean
  @Transformer(inputChannel="input", outputChannel="output")
  JsonToObjectTransformer jsonToObjectTransformer() {
    return new JsonToObjectTransformer(jsonObjectMapper());
  }
}