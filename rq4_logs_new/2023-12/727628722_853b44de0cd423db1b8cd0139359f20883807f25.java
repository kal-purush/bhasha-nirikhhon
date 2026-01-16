package com.spartanullnull.otil.domain.user.helper;

import com.spartanullnull.otil.domain.user.entity.*;
import com.spartanullnull.otil.domain.user.exception.*;
import org.springframework.beans.factory.annotation.*;

public final class UserRoleResolver {

    // ADMIN_TOKEN
    @Value("${admin.token}")
    private static String ADMIN_TOKEN;

    /**
     * 사용자권한을 판단
     *
     * @param admin      어드민인지에 대한 어드민 여부 입력값
     * @param adminToken 어드민 토큰 값
     * @return 사용자 권한값
     */
    public static UserRoleEnum resolveUserRole(boolean admin,
        String adminToken) {
        if (admin) {
            if (ADMIN_TOKEN.equals(adminToken)) {
                return UserRoleEnum.ADMIN;
            }
            throw new InvalidAdminKeyException("adminToken", adminToken);
        }
        return UserRoleEnum.USER;
    }
}