package ru.yandex.practicum.filmorate.controller;

import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.filmorate.model.Review;
import ru.yandex.practicum.filmorate.service.ReviewServiceImpl;
import ru.yandex.practicum.filmorate.validation.Create;
import ru.yandex.practicum.filmorate.validation.Update;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/reviews")
@Slf4j
@Validated
@RequiredArgsConstructor
public class ReviewController {
    private final ReviewServiceImpl service;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Review save(@Validated(Create.class) @RequestBody Review review) {
        Review newReview = service.save(review);
        log.info("добавление отзыва {}", review.getId());
        return newReview;
    }

    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    public Review update(@Validated(Update.class) @RequestBody Review review) {
        log.info("обновление отзыва ID {}", review.getId());
        return service.update(review);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public void delete(@PathVariable Integer id) {
        log.info("удаление отзыва ID {}", id);
        service.delete(id);
    }

    @GetMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public Review getById(@PathVariable Integer id) {
        log.info("получение отзыва ID {}", id);
        return service.getById(id);
    }

    @GetMapping("/filmId/{filmId}/count/{count}")
    @ResponseStatus(HttpStatus.OK)
    public List<Review> getReviewsByFilm(@RequestParam(value = "filmId") Long filmId,
                                         @RequestParam(value = "count", defaultValue = "10") @Positive Integer count) {
        log.info("получение отзывов по фильму");
        return service.getReviewsByFilmId(Optional.of(filmId), count);
    }

    @PutMapping("/{id}/like/{userId}")
    @ResponseStatus(HttpStatus.OK)
    public void addLike(@PathVariable Integer reviewId,@PathVariable Long userId) {
        log.info("добавление лайка отзыву id {}", userId);
        service.addLike(reviewId, userId);
    }

    @PutMapping("/{id}/dislike/{userId}")
    @ResponseStatus(HttpStatus.OK)
    public void addDislike(@PathVariable Integer reviewId,@PathVariable Long userId) {
        log.info("добавление дизлайка отзыву id {}", userId);
        service.addDislike(reviewId, userId);
    }

    @DeleteMapping("/{id}/like/{userId}")
    @ResponseStatus(HttpStatus.OK)
    public void removeLike(@PathVariable Integer reviewId,@PathVariable Long userId) {
        log.info("удаление лайка отзыву id {}", userId);
        service.removeLike(reviewId, userId);
    }

    @DeleteMapping("/{id}/dislike/{userId}")
    @ResponseStatus(HttpStatus.OK)
    public void removeDislike(@PathVariable Integer reviewId,@PathVariable Long userId) {
        log.info("удаление дизлайка отзыву id {}", userId);
        service.removeLike(reviewId, userId);
    }

}