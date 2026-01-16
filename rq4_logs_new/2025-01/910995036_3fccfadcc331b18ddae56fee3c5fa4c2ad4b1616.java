package com.example.lifeonhana.entity.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ArticleCategory {
    REAL_ESTATE("부동산에 관심이 많은 마음부자🎩"),
    INVESTMENT("투자에 관심이 많은 멋진 중년 🎩"),
    INHERITANCE_GIFT("상속에 관심이 많은 간지나는 중년 🎩"),
    TRAVEL("여행을 좋아하는 건강미 중년 ⛰️"),
    CULTURE("문화생활을 즐기는 센스있는 중년 🎭"),
    HOBBY("취미생활을 즐기는 활기찬 중년 🎨");

    private final String nicknameTemplate;

    public String generateNickname(String userName) {
        return userName + "님은 " + this.nicknameTemplate;
    }
} 