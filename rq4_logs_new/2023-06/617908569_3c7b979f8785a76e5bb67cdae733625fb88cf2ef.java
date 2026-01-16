package ru.yandex.practicum.filmorate.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.filmorate.model.Review;
import ru.yandex.practicum.filmorate.service.ReviewService;

import javax.validation.Valid;
import java.util.List;

@RestController
@RequestMapping("/reviews")
@RequiredArgsConstructor
@Slf4j
public class ReviewController {
    private final ReviewService reviewService;

    @PostMapping
    public Review add(@Valid @RequestBody Review review) {
        log.info("поступил запрос на добавление отзыва");
        return reviewService.add(review);
    }

    @PutMapping
    public Review update(@Valid @RequestBody Review review) {
        log.info("поступил запрос на обновление отзыва");
        return reviewService.update(review);
    }

    @DeleteMapping("/id")
    public void delete(@PathVariable int id) {
        log.info("поступил запрос на удаление отзыва");
        reviewService.delete(id);
    }

    @GetMapping("/id")
    public Review getById(@PathVariable long id) {
        log.info("поступил запрос получения отзыва с id " + id);
        return reviewService.getById(id);
    }

    @GetMapping
    public List getAllReviews(@RequestParam (defaultValue = "0") long filmId, @RequestParam (defaultValue = "10") int count) {
        log.info("поступил запрос на получение всех отзывов с заданными параметрами");
        return reviewService.getAllReviews(filmId, count);
    }

    @PutMapping("/{id}/like/{userId}")
    public void addLike(@PathVariable long id, @PathVariable long userId) {
        log.info("поступил запрос на добавление лайка к отзыву с id " + id + " от пользователя с id " + userId);
        reviewService.addLike(id, userId);
    }

    @DeleteMapping("/{id}/like/{userId}")
    public void deleteLike(@PathVariable long id, @PathVariable long userId) {
        log.info("поступил запрос на удаление лайка к отзыву с id " + id + " от пользователя с id " + userId);
        reviewService.deleteLike(id, userId);
    }

    @PutMapping("/{id}/dislike/{userId}")
    public void addDislike(@PathVariable long id, @PathVariable long userId) {
        log.info("поступил запрос на добавление дизлайка к отзыву с id " + id + " от пользователя с id " + userId);
        reviewService.addDislike(id, userId);
    }

    @DeleteMapping("/{id}/dislike/{userId}")
    public void deleteDislike(@PathVariable long id, @PathVariable long userId) {
        log.info("поступил запрос на удаление дизлайка к отзыву с id " + id + " от пользователя с id " + userId);
        reviewService.deleteDislike(id, userId);
    }
}