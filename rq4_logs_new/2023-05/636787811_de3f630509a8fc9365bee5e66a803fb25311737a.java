package com.solvd.elasticsearchmicroservice.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Document(indexName = "books")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Note {

    @Id
    private Long id;

    @Field(type = FieldType.Text, name = "description")
    private String description;

    @Field(type = FieldType.Text, name = "theme")
    private String theme;

    @Field(type = FieldType.Text, name = "tag")
    private String tag;

    @Field(type = FieldType.Long, name = "userId")
    private Long userId;

}