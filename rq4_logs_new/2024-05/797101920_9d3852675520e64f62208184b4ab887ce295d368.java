package com.example.EnjoyTripBackend.util;

import org.springframework.stereotype.Component;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

@Component
public class Encrypt {

    public String getSalt(){
        SecureRandom secureRandom = new SecureRandom();
        byte[] salt = new byte[20];

        secureRandom.nextBytes(salt);
        StringBuffer sb = new StringBuffer();
        for(byte b: salt){
            sb.append(String.format("%02x", b));
        }

        return sb.toString();
    }

    public String encode(String password, String salt){
        String result = "";
        try{
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update((password+salt).getBytes());
            byte[] passwordSalt = md.digest();

            StringBuffer sb = new StringBuffer();
            for(byte b : passwordSalt){
                sb.append(String.format("%02x",b));
            }
            result = sb.toString();

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}