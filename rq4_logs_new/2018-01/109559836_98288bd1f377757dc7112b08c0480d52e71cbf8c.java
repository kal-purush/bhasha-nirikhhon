package com.aspirine.global.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This class can find duplicate characters in a String and count the number of occurrences using Java
 *
 * @author yathish
 */
public class CustomMathWorker {

    private static final Logger LOG = LoggerFactory.getLogger(CustomMathWorker.class);

    public static void main(String[] args) {

        String input = "MANIAC";

        LOG.info("Printing input {}", input);

        Map <Character, Integer> charMap = new HashMap <>();

        char[] arr = input.toCharArray();

        for (char element : arr) {
            if (Character.isAlphabetic(element)) {
                if (charMap.containsKey(element)) {
                    charMap.put(element, charMap.get(element) + 1);
                } else {
                    charMap.put(element, 1);
                }
            }
        }

        LOG.info("Printing output {}", charMap);

    }

}