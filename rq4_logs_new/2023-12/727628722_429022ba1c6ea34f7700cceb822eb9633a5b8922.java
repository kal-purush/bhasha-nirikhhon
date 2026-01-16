package com.spartanullnull.otil.domain;

import com.spartanullnull.otil.domain.user.entity.*;
import java.lang.annotation.*;
import org.springframework.security.test.context.support.*;

@Retention(RetentionPolicy.RUNTIME)
@WithSecurityContext(factory = WithMockCustomUserSecurityContextFactory.class)
public @interface WithMockCustomUser {

    String accountId() default "abc123";

    String password() default "abcDEF1234!";

    String nickname() default "tony9898";

    String email() default "abcd123@gmail.com";

    UserRoleEnum role() default UserRoleEnum.USER;

}