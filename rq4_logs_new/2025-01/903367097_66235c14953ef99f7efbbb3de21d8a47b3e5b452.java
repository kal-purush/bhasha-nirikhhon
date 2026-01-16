package com.api.knowknowgram.payload.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

public class OauthLoginRequest {
	@NotBlank
	@Schema(description = "access token", example = "", required = true)
	private String token;

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}
}