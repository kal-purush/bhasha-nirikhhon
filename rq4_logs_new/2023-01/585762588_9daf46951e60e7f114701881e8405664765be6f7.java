package com.tazedaily.TAZEDaily.Service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.tazedaily.TAZEDaily.Domain.Bookmark;
import com.tazedaily.TAZEDaily.Domain.NewsArticle;
import com.tazedaily.TAZEDaily.Domain.User;
import com.tazedaily.TAZEDaily.Repository.BookmarkRepository;
import com.tazedaily.TAZEDaily.Repository.NewsArticleRepository;
import com.tazedaily.TAZEDaily.Repository.UserRepository;

@Service
public class BookmarkService {

    @Autowired
    BookmarkRepository bookmarkRepository;
    @Autowired
    NewsArticleRepository newsArticleRepository;
    @Autowired
    UserRepository userRepository;

    public NewsArticle getNewsArticle(Long id) {
        return newsArticleRepository.findArticleById(id);
    }

    public User getUserById(Long id) {
        return userRepository.findUserById(id);
    }
}