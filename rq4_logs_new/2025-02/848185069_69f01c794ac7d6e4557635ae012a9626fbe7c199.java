package com.rljj.chipservice.domain.post.dto;

import com.rljj.switchswitchcommon.exception.Code;

public class PostDataResponse<T> extends PostResponse {
    private final T data;

    private PostDataResponse(T data) {
        super(true, Code.OK.getCode(), Code.OK.getMessage());
        this.data = data;
    }

    private PostDataResponse(T data, String message) {
        super(true, Code.OK.getCode(), message);
        this.data = data;
    }

    public static <T> PostDataResponse<T> of(T data) {
        return new PostDataResponse<>(data);
    }

    public static <T> PostDataResponse<T> of(T data, String message) {
        return new PostDataResponse<>(data, message);
    }

    public static <T> PostDataResponse<T> empty() {
        return new PostDataResponse<>(null);
    }
}
