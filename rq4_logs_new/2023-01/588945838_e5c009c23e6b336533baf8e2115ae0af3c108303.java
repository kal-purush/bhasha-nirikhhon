package com.study.article.entity;

import jakarta.persistence.*;

@Entity
@Table
public class Article {

    @Id
    @SequenceGenerator(
            name = "pk_article",
            sequenceName = "article_sequence",
            allocationSize = 1
    )
    @GeneratedValue(
            strategy = GenerationType.SEQUENCE,
            generator = "article_sequence"
    )

    private Long articlePk;
    private String articleTitle;
    private String articleContents;
    private String articleWriter;
    private String apostDate;

    public Long getArticlePk() {
        return articlePk;
    }

    public void setArticlePk(Long articlePk) {
        this.articlePk = articlePk;
    }

    public String getArticleTitle() {
        return articleTitle;
    }

    public void setArticleTitle(String articleTitle) {
        this.articleTitle = articleTitle;
    }

    public String getArticleContents() {
        return articleContents;
    }

    public void setArticleContents(String articleContents) {
        this.articleContents = articleContents;
    }

    public String getArticleWriter() {
        return articleWriter;
    }

    public void setArticleWriter(String articleWriter) {
        this.articleWriter = articleWriter;
    }

    public String getApostDate() {
        return apostDate;
    }

    public void setApostDate(String apostDate) {
        this.apostDate = apostDate;
    }

    @Override
    public String toString() {
        return "Article{" +
                "articlePk=" + articlePk +
                ", articleTitle='" + articleTitle + '\'' +
                ", articleContents='" + articleContents + '\'' +
                ", articleWriter='" + articleWriter + '\'' +
                ", postDate='" + apostDate + '\'' +
                '}';
    }
}