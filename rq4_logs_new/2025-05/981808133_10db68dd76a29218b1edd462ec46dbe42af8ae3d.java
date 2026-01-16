package com.unionclass.memberservice.email.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.unionclass.memberservice.common.exception.BaseException;
import com.unionclass.memberservice.common.exception.ErrorCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum EmailTitle {

    VERIFY_CODE("이메일 인증코드 발송 메일입니다."),
    TEMPORARY_PASSWORD("임시 비밀번호 발급 메일입니다.")
    ;

    private final String emailTitle;

    @JsonValue
    public String getEmailTitle() {
        return emailTitle;
    }

    @JsonCreator
    public static EmailTitle fromString(String value) {
        for (EmailTitle emailTitle : EmailTitle.values()) {
            if (emailTitle.emailTitle.equals(value)) {
                return emailTitle;
            }
        }
        throw new BaseException(ErrorCode.INVALID_EMAIL_TITLE_VALUE);
    }
}