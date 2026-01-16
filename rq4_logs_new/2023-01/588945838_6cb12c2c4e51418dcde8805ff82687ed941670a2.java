package com.study.article.controller;

import com.study.article.dto.ArticleDto;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;

import java.util.List;

public interface OmniArticleAPI {
    @PostMapping(value = "/article/create")
    String createArticle(String title, String content, String writer, String postDate);

    @GetMapping(value = "/article/read")
    ArticleDto getArticle(Long articlePk);

    @GetMapping(value = "/article/list")
    List<ArticleDto> getArticleList();

    @PostMapping(value = "/article/update")
    String updateArticle(Long articlePk, String title, String content, String writer);

    @DeleteMapping(value = "/article/delete")
    public String deleteArticle(Long articlePk);
}