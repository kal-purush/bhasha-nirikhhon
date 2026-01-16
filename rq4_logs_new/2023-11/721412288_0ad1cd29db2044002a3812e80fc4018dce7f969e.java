package com.smyr.showmeyourrecipe.entity.comment;

import com.smyr.showmeyourrecipe.entity.User;
import com.smyr.showmeyourrecipe.entity.post.Post;
import com.smyr.showmeyourrecipe.entity.post.PostLikePK;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.LocalDateTime;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@EntityListeners(AuditingEntityListener.class)
public class CommentLike{
    @EmbeddedId
    private CommentLikeKey commentLikeKey;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id")
    @MapsId("userId")
    private User user;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "comment_id")
    @MapsId("commentId")
    private Comment comment;

    @CreatedDate
    private LocalDateTime createdAt;

    @Builder
    public CommentLike(User user, Comment comment) {
        this.user = user;
        this.comment = comment;
        this.commentLikeKey = CommentLikeKey.builder()
                .userId(user.getId())
                .commentId(comment.getCommentId())
                .build();
    }
}