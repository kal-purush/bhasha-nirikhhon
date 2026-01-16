package com.tazedaily.TAZEDaily.Service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.tazedaily.TAZEDaily.Domain.NewsArticle;
import com.tazedaily.TAZEDaily.Repository.NewsArticleRepository;

@Service
public class NewsArticleService {

    @Autowired
    NewsArticleRepository newsArticleRepository;

    public NewsArticle addLike(Long id) {
        NewsArticle newsArticle = newsArticleRepository.findArticleById(id);
        int likes = newsArticle.getLikes();
        newsArticle.setLikes(likes + 1);
        return newsArticleRepository.save(newsArticle);
    }
}