package com.metamong.mt.member.service.mailformatter;

import org.springframework.stereotype.Component;

import com.metamong.mt.global.mail.MailMessageFormatter;
import com.metamong.mt.global.mail.MailType;

@Component
public class IdNotificationMailMessageFormatter extends MailMessageFormatter {

    @Override
    protected String getMessageFormat() {
        return """
                회원님의 아이디는 {0} 입니다.
                """;
    }

    @Override
    public MailType getSupportedMailType() {
        return MailType.ID_NOTIFICATION;
    }
}