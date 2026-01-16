package com.example.demo.common;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import lombok.Getter;

@Getter
public class ApiResponse<T> {

    private final String    message;
    private final T         data;


    private ApiResponse(String message, T data) {
        this.message = message;
        this.data = data;
    }

    /** 메시지 + 데이터 응답 생성 */
    private static <T> ApiResponse<T> of(String message, T data) {
        return new ApiResponse<>(message, data);
    }

    /** 메시지만 있는 응답 생성 (data = null) */ 
    private static <T> ApiResponse<T> messageOnly(String message) {
        return new ApiResponse<>(message, null);
    }

    
    // ============================
    // 전역 공통 응답 호출부
    // ============================

    /**
     * 일반적인 200 OK 응답 반환
     * 
     * 주어진 메시지와 데이터를 포함한 JSON 본문을 생성합니다.
     */
    public static <T> ResponseEntity<ApiResponse<T>> ok(String message, T data) {
        return ResponseEntity.ok(of(message, data));
    }

    /**
     * 일반적인 200 OK 응답 반환
     * 
     * 메시지만 포함하고 data는 null
     * 주로 단순 성공 알림(비밀번호 변경 성공 등)
     */
    public static ResponseEntity<ApiResponse<Void>> ok(String message) {
        return ResponseEntity.ok(messageOnly(message));
    }

    /**
     * 리소스 생성 성공을 의미하는 201 Created 응답 반환
     * 
     * 메시지로 클라이언트에게 성공 안내만, data는 null
     * 회원가입, 채팅방 생성 등의 기능에 사용
     */
    public static ResponseEntity<ApiResponse<Void>> created(String message) {
        return ResponseEntity.status(HttpStatus.CREATED).body(messageOnly(message));
    }

}