package com.angkorsuntrix.demosynctrix.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class ArticleResponse {

    private Long id;
    private String title;
    private String slug;
    private String body;
    private boolean published;
    private Date publish_at;
    @JsonProperty("topic")
    private Topic topic;

    public ArticleResponse(final Article article, final Topic topic) {
        this.id = article.getId();
        this.title = article.getTitle();
        this.slug = article.getSlug();
        this.body = article.getBody();
        this.published = article.isPublished();
        this.publish_at = article.getPublish_at();
        this.topic = topic;
    }

    public ArticleResponse(Long id, String title, String slug, String body, boolean published, Date publish_at, Topic topic) {
        this.id = id;
        this.title = title;
        this.slug = slug;
        this.body = body;
        this.published = published;
        this.publish_at = publish_at;
        this.topic = topic;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getSlug() {
        return slug;
    }

    public void setSlug(String slug) {
        this.slug = slug;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public boolean isPublished() {
        return published;
    }

    public void setPublished(boolean published) {
        this.published = published;
    }

    public Date getPublish_at() {
        return publish_at;
    }

    public void setPublish_at(Date publish_at) {
        this.publish_at = publish_at;
    }

    public Topic getTopic() {
        return topic;
    }

    public void setTopic(Topic topic) {
        this.topic = topic;
    }
}