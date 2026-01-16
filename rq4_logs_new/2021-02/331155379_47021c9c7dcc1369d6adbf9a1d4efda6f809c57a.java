package com.example.capstoneweb.Controller;

import com.example.capstoneweb.model.Board;
import com.example.capstoneweb.model.Comment;
import com.example.capstoneweb.service.CommentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@CrossOrigin(origins = "http://localhost:3000")
@RestController
@RequestMapping("/api")
public class CommentController {
    @Autowired
    private CommentService commentService;

    //create comment
    @PostMapping("/board/comment")
    public Comment createComment(@RequestBody Comment comment){
        return commentService.createComment(comment);
    }
    //get comment
    @GetMapping("/board/comment/{num}")
    public List<Comment> getCommentByNum(
            @PathVariable Integer num){
        return  commentService.getComment(num);
    }
    // delete board
    @DeleteMapping("/board/comment/{no}")
    public ResponseEntity<Map<String, Boolean>> deleteCommentByNo(
            @PathVariable Integer no) {

        return commentService.deleteComment(no);
    }
}