package com.techeer.checkIt.domain.book.dto.Response;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class BookReadingRes {
  private Long id;
  private String title;
  private String author;
  private String publisher;
  private String coverImageUrl;
  private int height;
  private int width;
  private int pages;
  private int likes;
}