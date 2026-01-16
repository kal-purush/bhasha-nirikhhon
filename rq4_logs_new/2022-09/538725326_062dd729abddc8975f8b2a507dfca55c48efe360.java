package com.roundsToThree.Representations;

import java.nio.charset.StandardCharsets;

public class PersonRepresentation {
    // Value Representation VR_PN

    public String familyName;
    public String givenName;
    public String middleName;
    public String prefix;
    public String suffix;

    /*
    Note:
    For vet usage, familyName is the pet owner's family / organisation.
    givenName is maintained, middleName, prefix and suffix are not used.

    todo: implement this => '=' used to represent ideographic and phonetic representations
    I still dont understand it
     */

    public PersonRepresentation(String familyName, String givenName, String middleName, String prefix, String suffix) {
        this.familyName = familyName;
        this.givenName = givenName;
        this.middleName = middleName;
        this.prefix = prefix;
        this.suffix = suffix;
    }

    // PersonRepresentation from raw data in VR_PN item
    public PersonRepresentation(byte[] data) {
        if (data == null)
            return;

        // todo: support alternate charsets that the DICOM spec supports
        String name = new String(data, StandardCharsets.UTF_8);
        String[] segments = name.trim().split("\\^");
        if (segments.length > 0)
            familyName = segments[0];
        if (segments.length > 1)
            givenName = segments[1];
        if (segments.length > 2)
            middleName = segments[2];
        if (segments.length > 3)
            prefix = segments[3];
        if (segments.length > 4)
            suffix = segments[4];
    }


    @Override
    public String toString() {

        String out = "";
        if (familyName != null)
            out += familyName + ", ";
        if (prefix != null)
            out += prefix + " ";
        if (givenName != null) {
            out += givenName;
            // only add space if there will be a middle name
            // too, this way the suffix wont have a space in
            // front of the comma
            if (middleName != null)
                out += " ";
        }
        if (middleName != null)
            out += middleName;
        if (suffix != null)
            out += ", " + suffix;

        return out;
    }
}