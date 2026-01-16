package com.pandora.backend.service;

import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Service
public class EncryptionService {
    static String encrypt(String input)
            throws NoSuchAlgorithmException {

        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] messageDigest =
                md.digest(input.getBytes(StandardCharsets.UTF_8));

        return convertToHex(messageDigest);
    }
    private static String convertToHex(final byte[] messageDigest) {
        BigInteger bigint = new BigInteger(1, messageDigest);
        String hexText = bigint.toString(16);

        while (hexText.length() < 32)
            hexText = "0".concat(hexText);

        return hexText;
    }
}