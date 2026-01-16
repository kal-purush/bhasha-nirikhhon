package com.templetree.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author Lalith Mannur
 */
public final class EncryptionUtil {
    private EncryptionUtil() {
        throw new AssertionError("Suppress default constructor for noninstantiability");
    }

    // SHA-256 base hashing to encode the passwords
    public static String getHash(String password) {
        String hashedPassword = null;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(password.getBytes());
            hashedPassword = org.apache.commons.codec.binary.Base64.encodeBase64String(digest);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return hashedPassword;
    }
}