package com.moviezone.dai_api.model.dto;

public class RefreshTokenRequestDTO {
    private String refreshToken;
    private String accessToken;
    private String userId;

    public RefreshTokenRequestDTO() {
    }

    public RefreshTokenRequestDTO(String refreshToken, String accessToken, String userId) {
        this.refreshToken = refreshToken;
        this.accessToken = accessToken;
        this.userId = userId;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public void setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
    }

    public String getaccessToken() {
        return accessToken;
    }

    public void setaccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}