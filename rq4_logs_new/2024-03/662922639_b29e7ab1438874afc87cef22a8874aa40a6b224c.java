package com.happiday.Happi_Day.domain.service;

import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;

@SpringBootTest
@ActiveProfiles("test")
class MailServiceTest {

    @Autowired
    private MailService mailService;

    @Test
    void 임의6자리숫자생성_성공() {
        // given

        // when
        String result = mailService.createNumber();

        // then
        Assertions.assertThat(result.length()).isEqualTo(6);
    }

    @Test
    void 이메일전송_성공() {
        // given
        MimeMessage message = mailService.createMessage("test@email.com", "123456");

        // When
        String result = mailService.sendEmail("test@email.com", "123456");

        // Then
        Assertions.assertThat(result).isNotNull();
    }

    @Test
    void 이메일내용생성_성공() throws MessagingException, IOException {
        // given

        // when
        MimeMessage message = mailService.createMessage("test@email.com", "123456");

        // then
        Assertions.assertThat(message.getSubject()).isEqualTo("HappiDay 이메일 인증 코드입니다.");
        Assertions.assertThat(message.getContent()).isEqualTo("이메일 인증코드: 123456");
    }
}