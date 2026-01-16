package com.example.blogit.comment;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class CommentService {
    private final CommentRepository commentRepository;

    public CommentService(CommentRepository commentRepository) {
        this.commentRepository = commentRepository;
    }

    public Comment createComment(Comment comment) {
        return commentRepository.save(comment);
    }

    public List<Comment> getComments(UUID blogUUID) {
        return commentRepository.findAllByBlogUUID(blogUUID);
    }

    public Comment upvoteComment(UUID commentUUID) throws Exception {
        Optional<Comment> optionalComment = commentRepository.findByUuid(commentUUID);
        if(optionalComment.isEmpty()) throw new Exception("Comment with UUID: " + commentUUID + " does not exist");

        Comment comment = optionalComment.get();

        int currentUpvotes = comment.getUpvotes();
        currentUpvotes++;
        comment.setUpvotes(currentUpvotes);

        return commentRepository.save(comment);
    }
}