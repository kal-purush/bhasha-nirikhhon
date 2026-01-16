package pl.kozhanov.projectmanagementsystem.service.impl;

import org.springframework.stereotype.Service;
import pl.kozhanov.projectmanagementsystem.domain.Comment;
import pl.kozhanov.projectmanagementsystem.repos.CommentRepo;
import pl.kozhanov.projectmanagementsystem.service.CommentService;

@Service
public class CommentServiceImpl implements CommentService {

    private final CommentRepo commentRepo;

    public CommentServiceImpl(final CommentRepo commentRepo) {
        this.commentRepo = commentRepo;
    }

    public void deleteComment(Long commentId) {
        Comment cmt = commentRepo.getById(commentId);
        commentRepo.delete(cmt);
    }
}