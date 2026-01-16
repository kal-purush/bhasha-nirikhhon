package com.metamong.mt.global.mail;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.metamong.mt.global.mail.exception.UnsupportedMailTypeException;

public abstract class AbstractMailAgentBean implements MailAgent {
    private final Map<MailType, MailMessageFormatter> formatters;
    
    protected AbstractMailAgentBean(List<MailMessageFormatter> formatterBeans) {
        this.formatters = formatterBeans.stream()
                .collect(Collectors.toUnmodifiableMap(MailMessageFormatter::getSupportedMailType,
                        (v) -> v,
                        (k1, k2) -> {
                            throw new IllegalStateException("MailType이 중복됩니다. mailType=" + k1.getSupportedMailType()
                                    + ", class1=" + k1 + ", " + "class2=" + k2);
                        }));
    }
    
    protected final MailMessageFormatter getMessageFormatter(MailType mailType) {
        if (!this.formatters.containsKey(mailType)) {
            throw new UnsupportedMailTypeException("mailType을 지원하는 MailMessageFormatter 빈이 없습니다. mailType=" + mailType);
        }
        return this.formatters.get(mailType);
    }
}