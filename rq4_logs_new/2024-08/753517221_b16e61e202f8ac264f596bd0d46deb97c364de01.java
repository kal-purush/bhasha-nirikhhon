package com.itcook.cooking.domain.domains.like.domain.entity.validator;

import com.itcook.cooking.domain.domains.like.domain.entity.Liked;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

@Component
@RequiredArgsConstructor
public class LikedValidator {
    /**
     * 좋아요 생성 검증
     */
    public void validateAdd(Liked liked) {
        Assert.notNull(liked.getItCookUserId(), "좋아요 회원 정보가 누락되었습니다.");
        Assert.notNull(liked.getPostId(), "좋아요의 게시글 정보가 누락되었습니다.");
    }
}