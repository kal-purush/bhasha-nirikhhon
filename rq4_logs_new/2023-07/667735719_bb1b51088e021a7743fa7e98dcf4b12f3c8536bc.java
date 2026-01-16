package com.example.youtubeClone;

import com.example.youtubeClone.dto.Comment;
import com.example.youtubeClone.service.CommentService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class testDataInit {

    private final CommentService commentService;

    @PostConstruct
    private void init() {
        Comment comment1 = new Comment();
        comment1.setCommentContent("test1");
        comment1.setCommentName("test1");

        Comment comment2 = new Comment();
        comment2.setCommentContent("test2");
        comment2.setCommentName("test2");


        Comment comment3 = new Comment();
        comment3.setCommentContent("test3");
        comment3.setCommentName("test3");

        commentService.commentCreate(comment1, "test");
        commentService.commentCreate(comment2, "test");
        commentService.commentCreate(comment2, "test");
    }
}