package com.example.backend.api.data.dto.request;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class FestivalAPIRequest {

    private int start;
    private int end;
    private int size;

    public static FestivalAPIRequest healthCheckRequest() {
        return FestivalAPIRequest.builder()
            .start(1)
            .end(1)
            .size(0)
            .build();
    }

    public static FestivalAPIRequest toRequest(int index, int size) {
        return FestivalAPIRequest.builder()
            .start(index)
            .end(index)
            .size(size)
            .build();
    }
}