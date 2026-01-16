package com.example.parking.api.review;

import com.example.parking.application.review.ReviewService;
import com.example.parking.application.review.dto.ReviewCreateRequest;
import com.example.parking.config.argumentresolver.MemberAuth;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RestController
public class ReviewController {

    private final ReviewService reviewService;

    @PostMapping("/parkings/{parkingId}/reviews")
    public ResponseEntity<Long> createReview(@PathVariable Long parkingId, @MemberAuth Long memberId,
                                             @ModelAttribute ReviewCreateRequest request) {
        Long reviewId = reviewService.createReview(parkingId, memberId, request);
        return ResponseEntity.status(HttpStatus.CREATED).body(reviewId);
    }
}