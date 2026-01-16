package com.spartanullnull.otil.domain.user.helper;

import com.spartanullnull.otil.domain.user.exception.*;
import io.micrometer.common.util.*;
import jakarta.annotation.*;
import java.util.*;
import lombok.*;
import org.springframework.security.crypto.password.*;

@RequiredArgsConstructor
public final class PasswordResolver {

    private static PasswordEncoder passwordEncoder;
    private final PasswordEncoder wiredPasswordEncoder;

    /**
     * 수정요청 패스워드의 상태에 따라 인코딩값 혹은 비어있는 옵셔널 반환
     *
     * @param priorPassword         기존 패스워드
     * @param modifyRequestPassword 수정 요청 패스워드
     * @return 수정 요청 패스워드 인코딩값 혹은 비어있는 옵셔널
     */
    public static Optional<String> resolvePasswordOrNull(
        String priorPassword,
        String modifyRequestPassword
    ) {
        if (StringUtils.isNotEmpty(modifyRequestPassword)) {
            return encodeIfMatches(priorPassword, modifyRequestPassword);
        }
        return Optional.empty();
    }

    /**
     * 기존 패스워드와 수정요청 패스워드가 동일하다면 수정요청 패스워드를 인코딩하여 반환
     *
     * @param priorPassword         기존 패스워드
     * @param modifyRequestPassword 수정요청 패스워드
     * @return 수정요청 패스워드 인코딩값
     */
    private static Optional<String> encodeIfMatches(String priorPassword,
        String modifyRequestPassword) {
        if (passwordEncoder.matches(priorPassword, modifyRequestPassword)) {
            return Optional.of(passwordEncoder.encode(modifyRequestPassword));
        }
        throw new IncorrectAccountIdOrPasswordException("password input",
            modifyRequestPassword);
    }

    @PostConstruct
    public void init() {
        passwordEncoder = this.wiredPasswordEncoder;
    }
}