/*
 * Copyright (c) wheejuni tech 2018.
 *
 * Proudly developed by Hwi Jun Jeong,
 * Inspired by Bomee, the smartest puppy of the Galaxy.
 *
 * me@wheejuni.com
 * https://github.com/seulgiwendy
 */

package com.codesquad.library.domain.assembler;

import com.codesquad.library.domain.Book;
import com.codesquad.library.domain.BookCategories;
import com.codesquad.library.domain.BookStatus;
import com.codesquad.library.domain.Review;
import com.codesquad.library.dtos.model.book.BookDocument;
import com.codesquad.library.dtos.model.book.NewBookDocument;
import com.codesquad.library.dtos.model.review.ReviewDocument;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class BookAssembler {

    public BookDocument writeBookDocument(Book book) {
        List<Review> reviews = book.getReviews();

        List<BookStatus> statuses = BookStatus.generateStatus(book);
        List<ReviewDocument> reviewDocuments = writeReviewDocument(reviews);

        return new BookDocument(
                "book",
                book.getId(),
                book.getTitle(),
                book.getDescription(),
                book.getImageHref(),
                reviewDocuments,
                book.getAuthor(),
                book.isPossessed(),
                book.getSeriesNumber(),
                calculateAverageRatings(reviews),
                statuses,
                book.getBookCategories(),
                book.getBookLocations());
    }

    public Book generateObject(NewBookDocument document) {
        return Book.builder()
                .title(document.getTitle())
                .description(document.getDescription())
                .isbn(document.getIsbn())
                .bookCategories(BookCategories.findByLabel(document.getCategory()))
                .bookLocations(document.getLocation())
                .build();
    }

    private double calculateAverageRatings(List<Review> reviews) {
        return reviews.stream().mapToDouble(r -> r.getScore()).average().getAsDouble();
    }

    private List<ReviewDocument> writeReviewDocument(List<Review> reviews) {
        return reviews.stream().map(r -> r.generateDocument()).collect(Collectors.toList());
    }
}