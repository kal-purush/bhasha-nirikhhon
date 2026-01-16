package com.example.team258.domain.elasticsearch.application;

import com.example.team258.domain.elasticsearch.dto.ElasticBookResponseDto;
import com.example.team258.domain.member.dto.UserResponseDto;

public class ElasticBookResponse {

    private Long id;
    private String bookName;
    private String bookAuthor;

    private ElasticBookResponse() {
    }

    public ElasticBookResponse(Long id, String bookName, String bookAuthor) {
        this.id = id;
        this.bookName = bookName;
        this.bookAuthor = bookAuthor;
    }

    public static ElasticBookResponse from(ElasticBookResponseDto elasticBookResponseDto) {
        return new ElasticBookResponse(
                elasticBookResponseDto.getId(),
                elasticBookResponseDto.getBookName(),
                elasticBookResponseDto.getBookAuthor()
        );
    }

    public Long getId() {
        return id;
    }

    public String getBookName() {
        return bookName;
    }

    public String getBookAuthor() {
        return bookAuthor;
    }
}

