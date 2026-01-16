package com.reservibe.domain.useCase.review;

import com.reservibe.domain.gateway.client.FindClientInterface;
import com.reservibe.domain.gateway.review.CreateReviewInterface;
import com.reservibe.domain.input.review.CreateReviewInput;

public class CreateReviewUseCase implements CreateReviewInterface {
    private final FindClientInterface findClientInterface;
    private final CreateReviewInterface createReviewInterface;
    public CreateReviewUseCase(FindClientInterface findClientInterface, CreateReviewInterface createReviewInterface) {
        this.findClientInterface = findClientInterface;
        this.createReviewInterface = createReviewInterface;
    }

    public void createReview(CreateReviewInput reviewInput) {
        var client = this.findClientInterface.findClientById(reviewInput.clientId());

        var review = this.createReviewInterface.createReview();
    }

    @Override
    public Object createReview() {
        return null;
    }
}