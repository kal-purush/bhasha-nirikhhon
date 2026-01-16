package com.example.team258.common.entity;

import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
/**
 * 엘라스틱 서치에 사용되는 인덱스 입니다.(관계형 디비 table - 엘라스틱서치 index)
 * */

@Document(indexName = "book")
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
public class ElasticsearchBook {

    @Id
    @Field(name = "book_id", type = FieldType.Long)
    private Long bookId;

    @Field(type = FieldType.Text, name = "book_name")
    private String bookName;

    @Field(type = FieldType.Text, name = "book_author")
    private String bookAuthor;

    @Field(type = FieldType.Text, name = "book_publish")
    private String bookPublish;

    @Field(type = FieldType.Keyword, name = "book_status")
    private BookStatusEnum bookStatus;

}