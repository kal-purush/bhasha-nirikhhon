package com.study.article.service;

import com.study.article.dao.ArticleDao;
import com.study.article.dto.ArticleDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertNull;

@SpringBootTest
class ArticleServiceTest {

    @Autowired
    private ArticleService articleService;

    @Mock
    private ArticleDao articleDao;

    @BeforeEach
    void setUp() {
    }

    @Test
    void createArticle() {

    }

    @Test
    void getArticle() {


        final Long givenArticlePk = 1L;

        ArticleDto resultDto = articleService.getArticle(givenArticlePk);
        assertNull(resultDto);
        //assertNotNull(resultDto);
    }

    @Test
    void countArticleList() {
    }

    @Test
    void getArticleList() {
    }

    @Test
    void updateArticle() {
    }

    @Test
    void deleteArticle() {
    }

    @Test
    void updateAttachmentYn() {
    }
}