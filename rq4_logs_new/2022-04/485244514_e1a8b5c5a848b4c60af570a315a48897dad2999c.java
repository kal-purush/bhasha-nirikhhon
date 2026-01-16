package com.example.blogit.comment;

import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/blog/comments")
public class CommentController {
    @PostMapping("/create")
    public Comment createComment(@RequestBody Comment comment) {
        // let the comment service create a new comment in the DB
        return comment;
    }

    @GetMapping("/{blogUUID}")
    public void getComments(@PathVariable("blogUUID") UUID blogUUID) {
        // make the service get all comments that have a blog UUID.
        // return list of comments
    }
}