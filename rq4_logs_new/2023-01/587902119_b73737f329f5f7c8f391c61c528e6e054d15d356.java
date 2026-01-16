package africa.semicolon.Controllers;

import africa.semicolon.Data.Models.Comment;
import africa.semicolon.Services.CommentService;
import africa.semicolon.dtos.requests.CreateCommentRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class CommentController {
    @Autowired
    private CommentService commentService;
    @PostMapping("/comment")
    public String addComment(@RequestBody CreateCommentRequest createCommentRequest){
        commentService.createComment(createCommentRequest);
        return "Comment added";
    }
    @GetMapping("/comment/{postId}")
    public Comment viewComment(@PathVariable String postId){

        return commentService.viewComment(postId);
    }
}