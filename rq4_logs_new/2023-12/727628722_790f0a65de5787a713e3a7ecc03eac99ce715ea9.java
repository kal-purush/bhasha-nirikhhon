package com.spartanullnull.otil.domain.post.dto;

import com.spartanullnull.otil.domain.category.entity.*;
import com.spartanullnull.otil.domain.post.entity.*;
import com.spartanullnull.otil.global.dto.*;
import java.time.*;
import java.util.*;
import lombok.*;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PostCategoryResponseDto extends CommonResponseDto {

    private Long id;
    private String accountId;
    private String title;
    private String content;
    private List<String> categoryList;
    private LocalDateTime createdAt;

    public static PostCategoryResponseDto of(Post post, List<Category> categoryList) {
        return PostCategoryResponseDto.builder()
            .id(post.getId())
            .accountId(post.getUser().getAccountId())
            .title(post.getTitle())
            .content(post.getContent())
            .categoryList(
                categoryList.stream().map(Category::getCategoryName).toList())
            .createdAt(post.getCreatedAt())
            .build();
    }
    public static PostCategoryResponseDto of(Post post) {
        return PostCategoryResponseDto.builder()
            .id(post.getId())
            .accountId(post.getUser().getAccountId())
            .title(post.getTitle())
            .content(post.getContent())
            .categoryList(
                post.getPostCategories()
                    .stream()
                    .filter(postCategory -> postCategory.getPost().equals(post))
                    .map(PostCategory::getCategory)
                    .toList()
                    .stream().map(Category::getCategoryName)
                    .toList()
            )
            .createdAt(post.getCreatedAt())
            .build();
    }

}