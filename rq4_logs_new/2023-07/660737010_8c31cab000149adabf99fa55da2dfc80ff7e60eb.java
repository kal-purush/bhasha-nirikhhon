package com.example.oneulnail.domain.post.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Getter
@NoArgsConstructor
@Entity
@Table(name = "post")
public class Post {

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "name")
    private String name;

    @Column(name = "like_count")
    private int likeCount;

    @Column(name = "img_url")
    private String imgUrl;

    @Column(name = "price")
    private int price;

    @Column(name = "content")
    private String content;

    @Builder
    public Post(String name, int likeCount, String imgUrl, int price, String content) {
        this.name = name;
        this.likeCount = likeCount;
        this.imgUrl = imgUrl;
        this.price = price;
        this.content = content;
    }
}