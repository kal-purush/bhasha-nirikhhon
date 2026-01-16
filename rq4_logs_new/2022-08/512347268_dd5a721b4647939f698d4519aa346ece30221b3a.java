package com.cakestation.backend.review.fixture;

import com.cakestation.backend.review.domain.Distance;
import com.cakestation.backend.review.domain.Review;
import com.cakestation.backend.review.domain.ReviewTag;
import com.cakestation.backend.review.domain.Tag;
import com.cakestation.backend.review.dto.request.CreateReviewDto;

import java.util.List;

import static com.cakestation.backend.store.fixture.StoreFixture.storeEntity;
import static com.cakestation.backend.user.fixture.UserFixture.userEntity;

public class ReviewFixture {

    public static Long REVIEW_ID = 1L;

    public static Distance WALKING_DISTANCE = Distance.FIVE; // 도보 거리 (5, 10, 15, 15이상)

    public static String PHOTO_URL = "aaa"; // 리뷰 사진 url

    public static int CAKE_NUMBER = 1; // 케이크 호수

    public static String SHEET_TYPE = "초코"; // 시트 종류

    public static String REQUEST_OPTION = ""; // 추가 옵션

    public static int SATISFACTION = 5; // 만족도

    public static int SCORE = 5; // 별점

    public static List<Tag> TAGS = List.of(Tag.CHEAP);

    public static String CONTENT = ""; // 내용

    public static CreateReviewDto reviewDto(){

        return CreateReviewDto.builder()
                .walkingDistance(WALKING_DISTANCE)
                .photoUrl(PHOTO_URL)
                .cakeNumber(CAKE_NUMBER)
                .sheetType(SHEET_TYPE)
                .requestOption(REQUEST_OPTION)
                .satisfaction(SATISFACTION)
                .score(SCORE)
                .tags(TAGS)
                .content(CONTENT)
                .build();
    }

    public static Review reviewEntity(){

        ReviewTag reviewTag = ReviewTag.builder()
                .tag(Tag.CHEAP)
                .build();

        Review review = Review.builder()
                .id(REVIEW_ID)
                .walkingDistance(WALKING_DISTANCE)
                .photoUrl(PHOTO_URL)
                .cakeNumber(CAKE_NUMBER)
                .sheetType(SHEET_TYPE)
                .requestOption(REQUEST_OPTION)
                .satisfaction(SATISFACTION)
                .score(SCORE)
                .content(CONTENT)
                .tags(List.of(reviewTag))
                .writer(userEntity())
                .store(storeEntity())
                .build();

        reviewTag.setReview(review);

        return review;

    }
}