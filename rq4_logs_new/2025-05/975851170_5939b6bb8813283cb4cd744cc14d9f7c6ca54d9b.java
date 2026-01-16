package com.example.demo.util;

public class TokenUtils {
    private static final String BEARER_PREFIX = "Bearer ";

    /**
     * Authorization 헤더에서 'Bearer ' 접두어를 제거하고 토큰 본문만 반환합니다.
     * @param bearerHeader 전체 Authorization 헤더 값 (예: "Bearer eyJ...")
     * @return 실제 토큰 문자열 (예: "eyJ...")
     * @throws IllegalArgumentException 접두어가 없으면 예외 발생
     */
    public static String extractRefreshFrom(String bearerHeader) {
        if (bearerHeader == null || !bearerHeader.startsWith(BEARER_PREFIX)) {
            throw new IllegalArgumentException("Invalid Authorization header");
        }
        return bearerHeader.substring(BEARER_PREFIX.length());
    }    
}