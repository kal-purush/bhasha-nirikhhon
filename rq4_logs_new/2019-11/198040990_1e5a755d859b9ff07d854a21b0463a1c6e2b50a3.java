package com.kamilmarnik.talkerr.comment.domain;

import com.kamilmarnik.talkerr.comment.dto.CommentDto;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.time.LocalDateTime;

@Entity
@Table(name = "comments")
@Builder(toBuilder = true)
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
class Comment {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "comment_id")
  @Getter(AccessLevel.PACKAGE)
  Long commentId;

  @NotNull
  @Column(name = "content")
  String content;

  @Column(name = "created_on", columnDefinition = "TIMESTAMP WITH TIME ZONE NOT NULL")
  LocalDateTime createdOn;

  @NotNull
  @Column(name = "post_id")
  Long postId;

  @NotNull
  @Column(name = "author_id")
  Long authorId;

  static Comment fromDto(CommentDto dto) {
    return Comment.builder()
        .commentId(dto.getCommentId())
        .content(dto.getContent())
        .createdOn(dto.getCreatedOn())
        .postId(dto.getPostId())
        .authorId(dto.getAuthorId())
        .build();
  }

  CommentDto dto() {
    return CommentDto.builder()
        .commentId(commentId)
        .content(content)
        .createdOn(createdOn)
        .postId(postId)
        .authorId(authorId)
        .build();
  }
}