/*
 * Copyright (c) 2019 Thibault Helsmoortel.
 *
 * This file is part of numbered-strings.
 *
 * numbered-strings is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * numbered-strings is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with numbered-strings.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.github.thibstars.strings;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Thibault Helsmoortel
 */
@SuppressWarnings("WeakerAccess") // Methods should be available publicly
public class NumberedString {

    private NumberedString() {
        // Prevent instantiation
    }

    /**
     * Returns a formatted String with a number.
     *
     * @param format format String
     * @param number number
     * @param singularForm singular form
     * @param pluralForm plural form
     * @return formatted String with number
     */
    public static String format(String format, int number, String singularForm, String pluralForm) {
        return String.format(format, number, getNumberedString(number, singularForm, pluralForm));
    }

    /**
     * Returns a grammatically numbered String (singular/plural form of a word).
     *
     * @param number number
     * @param singularForm singular form of the word
     * @param pluralForm plural form of the word
     * @return grammatically numbered String
     */
    public static String getNumberedString(int number, String singularForm, String pluralForm) {
        return number == 1 ? singularForm : pluralForm;
    }

    /**
     * Returns a formatted String with a number.
     *
     * @param format format String
     * @param number number
     * @param pair pair of possible forms
     * @return formatted String with number
     */
    public static String format(String format, int number, NumberedStringPair pair) {
        return format(format, number, pair.getSingularForm(), pair.getPluralForm(), 0);
    }

    /**
     * Returns a formatted String with a number.
     *
     * @param format format String
     * @param number number
     * @param singularForm singular form
     * @param pluralForm plural form
     * @param numberIndex position of the number argument
     * @return formatted String with number
     */
    public static String format(String format, int number, String singularForm, String pluralForm, int numberIndex) {
        List<Object> objects = new ArrayList<>();
        objects.add(getNumberedString(number, singularForm, pluralForm));
        objects.add(numberIndex, number);

        return String.format(format, objects.toArray());
    }

    /**
     * Returns a formatted String with a number.
     *
     * @param format format String
     * @param number number
     * @param pairs list of pairs of possible forms
     * @return formatted String with number
     */
    public static String format(String format, int number, List<NumberedStringPair> pairs) {
        return format(format, number, pairs, 0);
    }

    /**
     * Returns a formatted String with a number.
     *
     * @param format format String
     * @param number number
     * @param pairs list of pairs of possible forms
     * @param numberIndex position of the number argument
     * @return formatted String with number
     */
    public static String format(String format, int number, List<NumberedStringPair> pairs, int numberIndex) {
        List<Object> objects = pairs.stream()
            .map(pair -> getNumberedString(number, pair.getSingularForm(), pair.getPluralForm()))
            .collect(Collectors.toList());
        objects.add(numberIndex, number);

        return String.format(format, objects.toArray());
    }
}