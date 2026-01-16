package ru.aaxee.homework2.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import ru.aaxee.homework2.domain.Comment;
import ru.aaxee.homework2.dto.Response;
import ru.aaxee.homework2.exception.LibraryException;
import ru.aaxee.homework2.service.CommentService;

@Log
@RequiredArgsConstructor
@ShellComponent
@ShellCommandGroup("Comments management")
public class CommentShellController {

    private final CommentService commentService;

    @ShellMethod("Add comment to book by id")
    public Response addComment(Long bookId, String text) throws LibraryException {
        Comment comment = commentService.addComment(bookId, text);
        return Response.of(comment);
    }

    @ShellMethod("Show all comments of book")
    public Response findAllComments(Long bookId) {
        return Response.of(commentService.findAllByBookId(bookId));
    }

}