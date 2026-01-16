package ru.otus.spring.service.impl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.MessageSource;
import org.springframework.stereotype.Service;
import ru.otus.spring.service.LocalizationService;

import java.util.Locale;

@Service
public class SimpleLocalizationService implements LocalizationService {

    private final String appLang;
    private final MessageSource messageSource;
    private final Locale locale;

    public SimpleLocalizationService(@Value("${messages.language}") String appLang, MessageSource messageSource) {
        this.appLang = appLang;
        this.messageSource = messageSource;
        this.locale = Locale.forLanguageTag(appLang);
    }

    public String getLocalString(String code,String... args) {


        return  messageSource.getMessage(code, args,  this.locale);
    }


}