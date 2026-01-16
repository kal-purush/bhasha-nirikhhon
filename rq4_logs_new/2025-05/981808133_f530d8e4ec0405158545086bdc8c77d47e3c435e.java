package com.unionclass.memberservice.email.dto.in;

import com.unionclass.memberservice.email.vo.in.EmailCodeReqVo;
import jakarta.validation.Valid;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class EmailCodeReqDto {

    private String email;
    private String verificationCode;

    @Builder
    public EmailCodeReqDto(String email, String verificationCode) {
        this.email = email;
        this.verificationCode = verificationCode;
    }

    public static EmailCodeReqDto from(EmailCodeReqVo emailCodeReqVo) {
        return EmailCodeReqDto.builder()
                .email(emailCodeReqVo.getEmail())
                .verificationCode(emailCodeReqVo.getVerificationCode())
                .build();
    }
}