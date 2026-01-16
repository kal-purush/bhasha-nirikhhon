package com.registration.util;

import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

@Service
public class StringEncryptor {
    private final static String ENCRYPT_METHOD = "DES";
    private final static String STRING_FORMAT = "UTF8";
    private static SecretKey key;

    private static Cipher ecipher;
    private static Cipher dcipher;

    @PostConstruct
    public void initialize() {
        try {
            SecretKey key = KeyGenerator.getInstance(ENCRYPT_METHOD).generateKey();
            ecipher = Cipher.getInstance(ENCRYPT_METHOD);
            ecipher.init(Cipher.ENCRYPT_MODE, key);
            dcipher = Cipher.getInstance(ENCRYPT_METHOD);
            dcipher.init(Cipher.DECRYPT_MODE, key);
        } catch (Exception e) {
            //logging problem with initialization encryptor
        }
    }
    public static String encrypt(String str) {
        try {
            // Encode the string into bytes using utf-8
            byte[] enc = ecipher.doFinal(str.getBytes(STRING_FORMAT));
            // Encode bytes to base64 to get a string
            return new sun.misc.BASE64Encoder().encode(enc);
        } catch(Exception e) {
        //logging problem with encrypting string
        }
        return null;
    }

    public static String decrypt(String str) {
        try {
            // Decode base64 to get bytes
            byte[] dec = new sun.misc.BASE64Decoder().decodeBuffer(str);
            byte[] utf8 = dcipher.doFinal(dec);

            // Decode using utf-8
            return new String(utf8, STRING_FORMAT);
        } catch(Exception e) {
            //logging problem with decrypting string
        }
        return null;
    }
}