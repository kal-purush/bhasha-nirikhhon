package com.angkorsuntrix.demosynctrix.payload;

import com.angkorsuntrix.demosynctrix.entity.Topic;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.util.HashMap;
import java.util.Map;

public class ArticleRequest {

    private Long id;

    @NotBlank
    @Size(min = 8, max = 100)
    private String title;

    @NotBlank
    @Size(min = 8, max = 100)
    private String slug;

    private String body;

    @NotBlank
    @Size(max = 500)
    private String description;

    private Topic topic;

    private Map<String, String> metaMap = new HashMap<>();

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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Topic getTopic() {
        return topic;
    }

    public void setTopic(Topic topic) {
        this.topic = topic;
    }

    public void setMetaMap(Map<String, String> metaMap) {
        this.metaMap = metaMap;
    }

    public Map<String, String> getMetaMap() {
        return this.metaMap;
    }
}