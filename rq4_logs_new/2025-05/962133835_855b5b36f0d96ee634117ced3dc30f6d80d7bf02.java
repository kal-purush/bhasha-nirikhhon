package ewm.comment.controller;

import ewm.comment.dto.CommentDto;
import ewm.comment.dto.NewCommentDto;
import ewm.comment.service.CommentService;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(path = "/users/{userId}/comments")
@RequiredArgsConstructor
@Slf4j
public class PrivateCommentController {
    private final CommentService commentService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CommentDto postComment(@PathVariable @NotNull @Positive Long userId,
                                  @RequestParam(required = true) @Positive Long eventId,
                                  @RequestBody NewCommentDto newCommentDto) {
        return commentService.postComment(userId, eventId, newCommentDto);
    }

    @PatchMapping
    @ResponseStatus(HttpStatus.OK)
    public CommentDto patchComment(@PathVariable @NotNull @Positive Long userId,
                                   @RequestParam(required = true) @Positive Long commentId,
                                   @RequestBody NewCommentDto newCommentDto) {
        return commentService.patchComment(userId, commentId, newCommentDto);
    }

    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteCat(@RequestParam(required = true) @Positive Long commentId) {
        commentService.deleteComment(commentId);
    }

}