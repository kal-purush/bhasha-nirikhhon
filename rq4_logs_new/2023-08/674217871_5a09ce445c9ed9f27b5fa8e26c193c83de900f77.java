package api.wantedpreonboardingbackend.global.dto;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum CustomErrorCode {
    F_001("F-001", "JSON 형식 오류"),
    F_002("F-002", "회원 인증 실패"),
    F_101("F-101", "이미 가입된 이메일"),
    F_102("F-102", "존재하지 않는 회원"),
    F_103("F-103", "비밀번호 오류"),
    F_104("F-104", "인증되지 않은 사용자"),
    F_105("F-105", "올바르지 않은 이메일 형식"),
    F_106("F-106", "8자 미만의 비밀번호"),
    F_107("F-107", "유효성 검사 실패"),
    F_201("F-201", "존재하지 않는 게시글"),
    F_202("F-202", "게시글 권한 변경 없음");

    private final String code;
    private final String message;

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}