package com.reservibe.application.controller.review;

import com.reservibe.domain.input.review.ReviewInput;
import com.reservibe.domain.usecase.review.RegisterReviewUseCase;
import com.reservibe.infra.adapter.reservation.SearchReservationAdapter;
import com.reservibe.infra.adapter.review.CreateReviewAdapter;
import com.reservibe.infra.repository.reservation.ReservationRepository;
import com.reservibe.infra.repository.review.ReviewRepository;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/review/create")
public class RegisterReviewController {

    private final ReviewRepository reviewRepository;
    private final ReservationRepository reservationRepository;

    public RegisterReviewController(ReviewRepository reviewRepository, ReservationRepository reservationRepository) {
        this.reviewRepository = reviewRepository;
        this.reservationRepository = reservationRepository;
    }

    @PostMapping
    public void createReview(@RequestBody ReviewInput input) {
        RegisterReviewUseCase registerReviewUseCase = new RegisterReviewUseCase(
                new CreateReviewAdapter(reviewRepository,
                new SearchReservationAdapter(reservationRepository)));
        registerReviewUseCase.execute(input);
    }
}