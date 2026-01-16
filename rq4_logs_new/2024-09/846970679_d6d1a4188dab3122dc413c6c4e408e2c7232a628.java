package com.onion.article.dto;


import com.onion.comment.dto.CommentDTO;
import java.util.List;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ArticleDetailDTO {
    private UUID articleId;
    private String title;
    private String content;
    private UUID userId;
    private String username;
    private UUID boardId;
    private String boardTitle;
    private List<CommentDTO> comments;
}