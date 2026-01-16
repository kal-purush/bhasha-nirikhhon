package com.example.demo.payment.entity;

public enum PaymentStatus {
    PAYMENT_READY, // 결제 대기
    PAYMENT_CONFIRMED, // 결제 확정
    CAFE_ACCEPTED, // 카페 승인
    CAFE_READY, // 캌페 준비
    PICK_UP_FINISHED, // 픽업 완료
    PAYMENT_CANCELED // 결제 취소
}