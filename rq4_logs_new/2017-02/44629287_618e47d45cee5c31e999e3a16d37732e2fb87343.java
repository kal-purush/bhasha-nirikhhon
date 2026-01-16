/*
 * This code has been entirely written by Thibault Helsmoortel.
 * Do not copy or (re)distribute without written permission.
 */

package be.thibaulthelsmoortel.helsmoortelUtil.util.text;

import org.apache.commons.lang3.StringUtils;

/**
 * Class responsible for abbreviating Strings.
 *
 * @author Thibault Helsmoortel
 */
public class TextAbbreviator {

    public static final String DEFAULT_SPLITTER = "|";

    /**
     * Abbreviates a given String with an important ending to a given maximum length using the default splitter.
     *
     * @param s the String to abbreviate
     * @param maxLength the maximum length of the abbreviated String
     * @return the abbreviated String
     */
    public static String abbreviateWithImportantEnding(String s, int maxLength) {
        // Abbreviates with default split char
        return abbreviateWithImportantEnding(s, maxLength, DEFAULT_SPLITTER);
    }

    /**
     * Abbreviates a given String with an important ending to a given maximum length using a given splitter.
     *
     * @param s the String to abbreviate
     * @param maxLength the maximum length of the abbreviated String
     * @param split the splitter String to determine the start of the ending
     * @return the abbreviated String
     */
    public static String abbreviateWithImportantEnding(String s, int maxLength, String split) {
        String result;
        int minAbbrevLength = 3; // Max length must be greater than 3 to abbreviate the start string with StringUtils

        if (s.contains(split) && maxLength > minAbbrevLength) {
            String start = s.substring(0, s.lastIndexOf(split)); // First part of the String
            String end = s.substring(s.lastIndexOf(split), s.length()); // End part of the String (important)

            int endLength = end.length();
            int maxStart = maxLength - endLength;
            if (start.length() > minAbbrevLength && maxStart > minAbbrevLength) {
                start = StringUtils.abbreviate(start, maxStart);
            } else {
                String ellipsis = "...";
                start = start.isEmpty() ? "" : ellipsis; // Only add ellipsis when the start is not empty
                if (end.length() > minAbbrevLength) {
                    end = StringUtils.abbreviate(end, maxLength - ellipsis.length());
                }
            }

            result = start + end;
        } else {
            result = StringUtils.abbreviate(s, maxLength);
        }

        return result;
    }
}