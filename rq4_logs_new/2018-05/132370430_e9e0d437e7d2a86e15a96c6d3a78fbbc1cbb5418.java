package com.nimtego.morse.impl;

import com.nimtego.morse.api.WordConversion;
import com.nimtego.morse.impl.converter.MorseArrayToByteArrayConverter;
import com.nimtego.morse.impl.converter.UnitConverter;

/**
 * Created by Pavel Gavrilov
 */
public abstract class AbstractWordConversion implements WordConversion {
    private UnitConverter<byte[], String[] > morseArrayToByteArrayConverter;

    public AbstractWordConversion() {
        morseArrayToByteArrayConverter = new MorseArrayToByteArrayConverter();
    }

    @Override
    public byte[] convert(String text) {
        return morseArrayToByteArrayConverter.from(convertToStringArray(text));
    }

    protected abstract String[] convertToStringArray(String text);
}