package com.metamong.mt.global.apispec;

import java.time.LocalDateTime;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * <p>공통 응답 템플릿.<br>
 * 컨트롤러에서 {@link ResponseEntity}를 반환할 때 이 객체를 반환하면 된다.
 * <p>예시:
 * <pre>
 * {@code
 * @GetMapping("/members/{memberId}")
 * public ResponseEntity<BaseResponse<MemberDto>> getMemberInfo(@PathVariable("memberId") int memberId) {
 *     MemberDto result = this.member.findMember(memberId);
 *     return BaseResponse.of(result, 200, "회원 조회 성공");
 * }
 * </pre>
 * @param <T> content의 타입
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(access = AccessLevel.PRIVATE)
@Getter
public class BaseResponse<T> {
    private final int statudCode;
    private final String message;
    private final LocalDateTime timestamp;
    private final T content;
    
    public static <T> BaseResponse<T> of(T content, int statusCode, String message) {
        return BaseResponse.<T>builder()
                .content(content)
                .statudCode(statusCode)
                .message(message)
                .timestamp(LocalDateTime.now())
                .build();
    }
    
    public static <T> BaseResponse<T> of(T content, HttpStatus status, String message) {
        return of(content, status.value(), message);
    }
    
    public static <T> BaseResponse<T> of(T content, int statusCode) {
        return of(content, statusCode, null);
    }
    
    public static <T> BaseResponse<T> of(T content, HttpStatus status) {
        return of(content, status.value());
    }
    
    public static <T> BaseResponse<T> of(int statusCode) {
        return of(null, statusCode);
    }
    
    public static <T> BaseResponse<T> of(HttpStatus status) {
        return of(status.value());
    }
    
    public static <T> BaseResponse<T> of(int statusCode, String message) {
        return of(null, statusCode, message);
    }
    
    public static <T> BaseResponse<T> of(HttpStatus status, String message) {
        return of(status.value(), message);
    }
}