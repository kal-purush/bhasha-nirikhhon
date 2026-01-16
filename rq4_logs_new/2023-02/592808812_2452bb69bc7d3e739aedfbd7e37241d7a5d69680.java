package com.revature.services;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class ProfanityService {
    public String filterProfanity(String text) {
        String[] profanity = {"fuck", "damn", "hell", "cum", "shit","bitch","tits"};
        for (String word : profanity) {
            text = text.replaceAll("(?i)" + word, asterisks(word.length()));
        }
        return text;
    }

    private String asterisks(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append("*");
        }
        return sb.toString();
    }
}