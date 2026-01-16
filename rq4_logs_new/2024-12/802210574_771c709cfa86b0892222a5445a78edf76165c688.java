package com.shortthirdman.primekit.essentials.common.util;

/**
 * @apiNote Utility class for operations on LDAP
 * @author shortthirdman
 * @since 1.0
 */
public final class LDAPUtils {

    private LDAPUtils() {}

    /**
     * Escaping Distinguished Name (DN) to prevent injection attacks.
     * @param name the DN to escape
     * @return the escaped DN
     */
    public static String escapeDN(String name) {
        StringBuilder sb = new StringBuilder(); // If using JDK >= 1.5 consider using StringBuilder
        if ((!name.isEmpty()) && ((name.charAt(0) == ' ') || (name.charAt(0) == '#'))) {
            sb.append('\\'); // add the leading backslash if needed
        }
        for (int i = 0; i < name.length(); i++) {
            char curChar = name.charAt(i);
            switch (curChar) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case ',':
                    sb.append("\\,");
                    break;
                case '+':
                    sb.append("\\+");
                    break;
                case '"':
                    sb.append("\\\"");
                    break;
                case '<':
                    sb.append("\\<");
                    break;
                case '>':
                    sb.append("\\>");
                    break;
                case ';':
                    sb.append("\\;");
                    break;
                default:
                    sb.append(curChar);
            }
        }
        if ((name.length() > 1) && (name.charAt(name.length() - 1) == ' ')) {
            sb.insert(sb.length() - 1, '\\'); // add the trailing backslash if needed
        }
        return sb.toString();
    }

    /**
     * Escaping search filter to prevent injection attacks.
     * @param filter the search filter to escape
     * @return the escaped string form of search-filter
     */
    public static String escapeSearchFilter(String filter) {
        StringBuffer sb = new StringBuffer(); // If using JDK >= 1.5 consider using StringBuilder
        for (int i = 0; i < filter.length(); i++) {
            char curChar = filter.charAt(i);
            switch (curChar) {
                case '\\':
                    sb.append("\\5c");
                    break;
                case '*':
                    sb.append("\\2a");
                    break;
                case '(':
                    sb.append("\\28");
                    break;
                case ')':
                    sb.append("\\29");
                    break;
                case '\u0000':
                    sb.append("\\00");
                    break;
                default:
                    sb.append(curChar);
            }
        }
        return sb.toString();
    }
}