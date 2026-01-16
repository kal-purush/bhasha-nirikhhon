package uno.csci4830.mavitapi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uno.csci4830.mavitapi.payload.request.comment.CreateCommentRequest;
import uno.csci4830.mavitapi.service.CommentService;

@RestController
@RequestMapping("/api/comment")
public class CommentController {

    @Autowired
    private CommentService commentService;

    @PostMapping("/create")
    ResponseEntity<?> createComment(@RequestBody CreateCommentRequest createCommentRequest) {
        try {
            return ResponseEntity.ok(commentService.createComment(createCommentRequest));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }

//    @PostMapping("/delete")
//    ResponseEntity<?> deleteComment(@RequestBody )

    @GetMapping("/getAllComments")
    ResponseEntity<?> getAllComments() {
        try {
            return ResponseEntity.ok(commentService.getAllComments());
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }

}