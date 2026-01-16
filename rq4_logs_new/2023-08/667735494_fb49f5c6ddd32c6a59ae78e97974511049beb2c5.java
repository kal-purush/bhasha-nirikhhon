package ru.practicum.comment.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import ru.practicum.comment.service.CommentService;
import ru.practicum.dto.CommentDto;
import ru.practicum.enums.AdminCommentStateAction;
import ru.practicum.enums.CommentState;

import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;
import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping(path = "/admin/comments")
@RequiredArgsConstructor
@Slf4j
public class AdminCommentController {

    private final CommentService commentService;

    @GetMapping
    public List<CommentDto> getComments(@RequestParam(required = false) List<Long> authors,
                                        @RequestParam(required = false) List<CommentState> states,
                                        @RequestParam(required = false) List<Long> events,
                                        @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
                                        LocalDateTime rangeStart,
                                        @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
                                        LocalDateTime rangeEnd,
                                        @RequestParam(defaultValue = "0") @PositiveOrZero Integer from,
                                        @RequestParam(defaultValue = "10") @Positive Integer size) {
        log.info("Получить список комментариев по указанным критериям: {}, {}, {}, {}, {}",
                authors,
                states,
                events,
                rangeStart,
                rangeEnd);
        return commentService.getComments(authors,
                states,
                events,
                rangeStart,
                rangeEnd,
                from,
                size);
    }

    @GetMapping("/{id}")
    public CommentDto getCommentById(@PathVariable Long id) {
        log.info("Получить комментарий по ID: {}", id);
        return commentService.getCommentById(id);
    }

    @PatchMapping("/{id}")
    public CommentDto moderateComment(@PathVariable Long id,
                                      @RequestParam AdminCommentStateAction action) {
        log.info("Решение о публикации комментария с ID: {} следующее: {}", id, action);
        return commentService.moderateComment(id, action);
    }
}