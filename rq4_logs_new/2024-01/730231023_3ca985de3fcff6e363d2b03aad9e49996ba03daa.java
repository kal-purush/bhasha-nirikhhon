package com.walkbuddies.backend.bookmark.dto;

import com.walkbuddies.backend.bookmark.domain.BookmarkEntity;
import lombok.*;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BookmarkResponse {

    private String bookmarkName;
    private String townName;
    private String nickName;
    private LocalDateTime regDate;

    public static BookmarkResponse of(BookmarkEntity bookmarkEntity) {

        return BookmarkResponse.builder()
                .bookmarkName(bookmarkEntity.getBookmarkName())
                .townName(bookmarkEntity.getTown().getTownName())
                .nickName(bookmarkEntity.getMember().getNickname())
                .regDate(bookmarkEntity.getRegDate())
                .build();
    }
}