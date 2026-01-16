package com.bensherriff;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Cipher {

    private final static Logger logger = LogManager.getLogger(Cipher.class);

    static String encrypt(String msg, int shift) {
        char[] ch = new char[msg.length()];
        for (int i = 0; i < msg.length(); i++) {
            ch[i] = (char) (msg.charAt(i) + Math.abs(shift%26));
        }
        return new String(ch);
    }

    static String decrypt(String msg, int shift) {
        char[] ch = new char[msg.length()];
        for (int i = 0; i < msg.length(); i++) {
            ch[i] = (char) (msg.charAt(i) - Math.abs(shift%26));
        }
        return new String(ch);
    }

    static String crack(String msg) {
        return msg;
    }
}