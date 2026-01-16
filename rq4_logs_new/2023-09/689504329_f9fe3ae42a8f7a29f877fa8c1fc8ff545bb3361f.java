package com.mega.common.validator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

class EmailValidatorTest {

    EmailValidator validator = new EmailValidator();
    @Test
    void test01() {
        String email1 = "dcwang@naver.com";
        String validate = validator.validate(email1);
        assertThat(validate).isEqualTo(email1);
    }

    @Test
    void test02() {
        String wrongEmail = "1234";

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            validator.validate(wrongEmail);
        });
        
        assertThat(exception.getMessage()).isEqualTo("유효하지 않은 이메일 주소임");
    }
}