package com.nimtego.morse.impl;

import com.nimtego.morse.impl.alphabet.EnglishAlphabetNoStream;
import com.nimtego.morse.impl.converter.TextToMorseArrayNoStreamConverter;
import com.nimtego.morse.impl.converter.UnitConverter;

/**
 * Created by Pavel Gavrilov
 */
public class EnglishNoStreamWordConversion extends AbstractWordConversion {
    private UnitConverter<String[], String> textToMorseArrayConverter;

    public EnglishNoStreamWordConversion() {
        textToMorseArrayConverter = new TextToMorseArrayNoStreamConverter(new EnglishAlphabetNoStream());
    }

    @Override
    protected String[] convertToStringArray(String text) {
        return textToMorseArrayConverter.from(text);
    }
}