package com.example.parking.auth.authcode;

import java.util.Arrays;
import lombok.Getter;

@Getter
public enum AuthCodeCategory {

    SIGN_UP("signUp"),
    FIND_PASSWORD("findPassword");

    private final String categoryName;

    AuthCodeCategory(String categoryName) {
        this.categoryName = categoryName;
    }

    public static AuthCodeCategory find(String authCodeCategory) {
        return Arrays.stream(AuthCodeCategory.values())
                .filter(category -> category.categoryName.equals(authCodeCategory))
                .findAny()
                .orElseThrow(() -> new InValidAuthCodeException("존재하지 않는 인증 코드 발급 행위입니다."));
    }
}