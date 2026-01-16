package com.example.event.domain;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.event.domain.value.Email;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class EmailTest {

    @Test
    @DisplayName("이메일 주소가 같으면 같은 객체로 판단한다.")
    void emailValidateSuccessfully() {

        //given
        String Email = "abc@abc.com";

        //when
        com.example.event.domain.value.Email email1 = new Email(Email);
        Email email2 = new Email(Email);

        //then
        assertThat(email1).isEqualTo(email2);
    }
}