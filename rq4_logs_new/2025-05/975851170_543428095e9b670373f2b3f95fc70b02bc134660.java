package com.example.demo.domain; // 또는 com.example.demo.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

// --- 수정된 부분: UserRole enum 정의 ---
@AllArgsConstructor // 각 enum 상수에 값을 주입하기 위해 필요
@Getter // roleName 값을 가져오기 위한 getter
public enum UserRole {
    // 각 역할의 이름과 설명을 정의
    ROLE_USER("ROLE_USER", "일반 사용자 권한"),
    ROLE_ADMIN("ROLE_ADMIN", "관리자 권한");

    private final String roleName; // Spring Security에서 사용하는 실제 권한 이름 (예: "ROLE_USER")
    private final String description; // 역할에 대한 설명

}