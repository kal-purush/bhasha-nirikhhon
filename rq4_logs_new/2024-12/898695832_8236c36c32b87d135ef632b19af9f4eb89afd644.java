package ru.clevertec.carshowroom.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import ru.clevertec.carshowroom.dto.review.ReviewRequest;
import ru.clevertec.carshowroom.dto.review.ReviewResponse;
import ru.clevertec.carshowroom.dto.review.UpdateReviewRequest;
import ru.clevertec.carshowroom.service.ReviewService;

import java.util.List;

@RestController
@Validated
@RequiredArgsConstructor
@RequestMapping("/reviews")
public class ReviewController {
    private final ReviewService reviewService;

    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public List<ReviewResponse> getAllReviews() {
        return reviewService.findAll();
    }

    @GetMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public ReviewResponse getReviewById(@PathVariable("id") Long id) { //@NotBlank
        return reviewService.findById(id);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ReviewRequest addReview(@RequestBody @Valid ReviewRequest reviewRequest) {
        return reviewService.addReview(reviewRequest);
    }

    @PutMapping()
    @ResponseStatus(HttpStatus.CREATED)
    public UpdateReviewRequest update(@RequestBody @Valid UpdateReviewRequest updateReviewRequest) {
        return reviewService.update(updateReviewRequest);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteById(@PathVariable("id") @Valid @NotBlank Long id) {
        reviewService.deleteById(id);
    }
}
