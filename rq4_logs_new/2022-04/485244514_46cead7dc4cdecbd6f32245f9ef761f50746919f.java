package com.example.blogit.blog;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.LocalDateTime;
import java.util.UUID;

@Table
@Entity
public class Blog {
    @Id

    private UUID uuid;
    private Long authorId;
    private String title;
    private String content;
    private String bannerImg;
    private LocalDateTime publishDate;

    public Blog() {
        this.uuid = UUID.randomUUID();
    }

    public Blog(Long authorId, String title, String content, String bannerImg, LocalDateTime publishDate) {
        this.uuid = UUID.randomUUID();
        this.authorId = authorId;
        this.title = title;
        this.content = content;
        this.bannerImg = bannerImg;
        this.publishDate = publishDate;
    }

    public UUID getUuid() {
        return uuid;
    }

    public Long getAuthorId() {
        return authorId;
    }

    public String getTitle() {
        return title;
    }

    public String getContent() {
        return content;
    }

    public String getBannerImg() {
        return bannerImg;
    }

    public LocalDateTime getPublishDate() {
        return publishDate;
    }

    @Override
    public String toString() {
        return "Blog{" +
                "uuid=" + uuid +
                ", authorId=" + authorId +
                ", title='" + title + '\'' +
                ", content='" + content + '\'' +
                ", bannerImg='" + bannerImg + '\'' +
                ", publishDate=" + publishDate +
                '}';
    }
}