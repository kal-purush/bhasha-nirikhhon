package com.tazedaily.TAZEDaily.Controller;

import java.time.LocalDateTime;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import com.tazedaily.TAZEDaily.Domain.Comment;
import com.tazedaily.TAZEDaily.Repository.CommentRepository;
import com.tazedaily.TAZEDaily.Service.CommentService;

@Controller
@RequestMapping("/comment")
public class CommentController {

    @Autowired
    private CommentRepository commentRepository;
    @Autowired
    private CommentService commentService;

    @GetMapping
    public ResponseEntity<Iterable<Comment>> getAllComments() {
        return new ResponseEntity<>(commentService.getAll(), HttpStatus.OK);
    }

    @PostMapping
    public ResponseEntity<Comment> create(@RequestBody Comment comment) {
        comment.setNewsArticle(commentService.getNewsArticle(1L));
        comment.setUser(commentService.getUserById(1L));
        comment.setTimeStamp(LocalDateTime.now());
        return new ResponseEntity<>(commentRepository.save(comment), HttpStatus.CREATED);
    }

    @DeleteMapping("/{commentId}")
    public ResponseEntity<Comment> delete(@PathVariable Long commentId) {
        return new ResponseEntity<>(HttpStatus.OK);
    }
}