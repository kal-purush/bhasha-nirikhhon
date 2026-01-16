package com.java.codeFront.controller;

import com.java.codeFront.model.News;
import com.java.codeFront.service.NewsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/news")
public class NewsRestController {

    @Autowired
    private NewsService newsService;

    @GetMapping
    public List<News> getAllNews() {
        return newsService.getAllNews();
    }

    @PostMapping
    public News addNews(@RequestBody News news) {
        return newsService.saveNews(news);
    }
}