package com.roundsToThree.Representations;

import com.roundsToThree.Structures.ValueRepresentation;

import java.nio.charset.StandardCharsets;

public class Representation {
    public byte[] value;

    // Value Representation of this class
    private static final ValueRepresentation valueRepresentation = ValueRepresentation.VALUE_REPRESENTATION_UN;

    public Representation() {
    }

    @Override
    public String toString() {
        String returnString = "";
        if (value == null || value.length == 0)
            return "N/A";
        else {
            String str = (new String(value, StandardCharsets.UTF_8));
            returnString += str.substring(0, Math.min(str.length(), 32));
            if (returnString.length() == 32 && value.length > 32)
                returnString += "...";
        }

        return returnString;
    }

    public ValueRepresentation getValueRepresentation() {
        return valueRepresentation;
    }

}