package org.iromu.ai.utils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class TokenUtils {

    /**
     * Count tokens in the given text by splitting it into words based on whitespace.
     *
     * @param text the message content
     * @return the token count
     */
    public static int countTokens(String text) {
        // Simple token count by splitting the content based on whitespace
        if (text == null || text.isEmpty()) {
            return 0;
        }
        // Split by whitespace and count the number of tokens
        return text.trim().split("\\s+").length;
    }
}