package com.sample.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MockBuilder {
    private static final ObjectMapper mapper = new ObjectMapper();
    public static Article getArticle(){
        Article article = new Article();
        article.setTitle("AnyTitle");
        article.setContent("Welcome to article");
        return article;
    }

    public static String getArticleAsJsonString() throws JsonProcessingException {
        return mapper.writeValueAsString(getArticle());
    }
}