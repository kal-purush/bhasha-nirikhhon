package com.spartanullnull.otil.domain.comment.entity;

import com.fasterxml.jackson.annotation.*;
import com.spartanullnull.otil.domain.like.entity.*;
import com.spartanullnull.otil.domain.post.entity.*;
import com.spartanullnull.otil.domain.user.entity.*;
import jakarta.persistence.*;
import java.util.*;
import lombok.*;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Builder
@Table(name = "comment")
public class Comment {

    @JsonIgnore
    @OneToMany(targetEntity = Like.class, mappedBy = "comment", cascade = CascadeType.ALL, orphanRemoval = true)
    private final List<Like> likes = new ArrayList<>();

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String commentText; // 댓글 내용

    @JsonIgnore
    @ManyToOne(fetch = FetchType.LAZY)
    private User user;

    @ManyToOne(fetch = FetchType.LAZY)
    private Post post;
}