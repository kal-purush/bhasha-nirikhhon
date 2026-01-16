package com.dateplan.dateplan.global.dto.response;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ApiResponse<T> {

	private boolean success;
	private T data;
	private String code;
	private String message;

	public static ApiResponse<Void> ofSuccess() {
		return new ApiResponse<>(true, null, null, null);
	}

	public static <T> ApiResponse<T> ofSuccess(T data) {
		return new ApiResponse<>(true, data, null, null);
	}
}