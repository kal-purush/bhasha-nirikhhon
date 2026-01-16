package com.gxdxx.instagram.dto.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

public record FeedResponse(

        @Schema(description = "다음 커서")
        @JsonProperty("next_cursor")
        Long nextCursor,

        @Schema(description = "피드 목록")
        List<PostFeedResponse> feeds

) {

    public static FeedResponse of(Long nextCursor, List<PostFeedResponse> feeds) {
        return new FeedResponse(nextCursor, feeds);
    }

}