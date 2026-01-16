package com.projects.breakingbook.business.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Book {
    private Long id;
    private String title;
    private List<String> authors;
    private String isbn;
    private String image;
    private String language;
    private String publisher;
    private Date datePublished;
    private int pages;
    private String synopsis;
    private boolean owned;
    private BookStatus status;
    private int rating;
    private String comment;
    private Friend friend;
    private User user;
}