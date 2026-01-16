package com.auth.auth.models;

import jakarta.persistence.*;

import java.time.LocalDate;
@Entity
public class Post {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    long id;
    String body;
    LocalDate CreatedAt;
    @ManyToOne
    SiteUser siteUser;

    public Post(String body, LocalDate createdAt, SiteUser siteUser) {
        this.body = body;
        CreatedAt = createdAt;
        this.siteUser = siteUser;
    }

}