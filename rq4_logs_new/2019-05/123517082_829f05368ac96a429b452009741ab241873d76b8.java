package de.lukaskoerfer.gradle.debugging.util;

import lombok.experimental.UtilityClass;
import org.codehaus.groovy.runtime.StringGroovyMethods;

@UtilityClass
public class StringUtil {

    public static String capitalize(String value) {
        return StringGroovyMethods.capitalize((CharSequence)value);
    }

    public static String uncapitalize(String value) {
        return StringGroovyMethods.uncapitalize(value);
    }

}