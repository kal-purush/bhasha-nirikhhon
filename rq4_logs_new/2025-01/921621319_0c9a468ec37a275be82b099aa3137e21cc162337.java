package com.metamong.mt.global.config;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.metamong.mt.global.mail.MailAgent;
import com.metamong.mt.global.mail.MailAgentMock;
import com.metamong.mt.global.mail.MailMessageFormatter;
import com.metamong.mt.global.web.cookie.CookieGenerator;
import com.metamong.mt.global.web.cookie.DefaultCookieGenerator;

@Configuration
public class BeanConfig {

    @Bean
    @ConditionalOnMissingBean(CookieGenerator.class)
    public DefaultCookieGenerator defaultCookieGenerator(@Value("${client.domain}") String clientDomain) {
        return new DefaultCookieGenerator(clientDomain);
    }
    
    @Bean
    @ConditionalOnMissingBean(MailAgent.class)
    public MailAgentMock mailAgentMock(List<MailMessageFormatter> mailMessageFormatters) {
        return new MailAgentMock(mailMessageFormatters);
    }
}