package com.cakestation.backend.review.dto.request;

import com.cakestation.backend.review.domain.Distance;
import com.cakestation.backend.review.domain.ReviewTag;
import com.cakestation.backend.review.domain.Tag;
import lombok.*;

import javax.persistence.*;
import java.util.List;

@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Getter
@Builder
public class CreateReviewDto {

    @Enumerated(value = EnumType.ORDINAL)
    private Distance walkingDistance; // 도보 거리 (5, 10, 15, 15이상)

    private String photoUrl; // 리뷰 사진 url

    private int cakeNumber; // 케이크 호수

    private String sheetType; // 시트 종류

    private String requestOption; // 추가 옵션

    private int satisfaction; // 만족도

    private int score; // 별점

    private List<Tag> tags;

    private String content; // 내용
}