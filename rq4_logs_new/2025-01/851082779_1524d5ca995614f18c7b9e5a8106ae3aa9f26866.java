package com.reddot.app.assembler;

import com.reddot.app.dto.response.BookmarkDTO;
import com.reddot.app.entity.Bookmark;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.springframework.stereotype.Component;

@Mapper(componentModel = "spring")
@Component
public interface BookmarkAssembler {
    @Mapping(target = "username", source = "user.username")
    @Mapping(target = "description", expression = "java(mapDescription(bookmark))")
    @Mapping(target = "title", expression = "java(mapTitle(bookmark))")
    BookmarkDTO toBookmarkDTO(Bookmark bookmark);

    // description is body of question if question is not null otherwise it is body of comment
    default String mapDescription(Bookmark bookmark) {
        if (bookmark.getQuestion() != null) {
            return bookmark.getQuestion().getBody();
        } else {
            return bookmark.getComment().getText();
        }
    }

    default String mapTitle(Bookmark bookmark) {
        if (bookmark.getQuestion() != null) {
            return bookmark.getQuestion().getTitle();
        } else {
            return bookmark.getComment().getText();
        }
    }
}