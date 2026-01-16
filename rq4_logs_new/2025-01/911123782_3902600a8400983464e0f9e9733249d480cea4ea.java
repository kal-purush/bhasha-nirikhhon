package com.roomfit.be.auth.domain;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.TimeToLive;

@RedisHash(value = "verification_code")
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class VerificationCode {
    @Id
    String uuid;

    String code;

    @Builder.Default
    @TimeToLive
    Long expiration = 180L;

    @Builder.Default
    Boolean isEmailVerified = false;
    public static VerificationCode of(String uuid, String code){
        return VerificationCode.builder()
                .uuid(uuid)
                .code(code)
                .build();
    }

    public void markEmailVerified() {
        isEmailVerified = true;
    }
    public boolean isCodeValid(String inputCode) {
        if (this.code != null && this.code.equals(inputCode)) {
            markEmailVerified();
            return true;
        }
        return false;
    }
}