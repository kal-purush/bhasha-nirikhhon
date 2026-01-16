package com.damda.user.service;

import com.damda.user.entity.User;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Base64;

@Component
public class PasswordEncoder {

    public String encrypt(String userId, String password) {
        try {
            KeySpec spec = new PBEKeySpec(password.toCharArray(), getSalt(userId), 85319, 128);
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");

            byte[] hash = factory.generateSecret(spec).getEncoded();
            return Base64.getEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException |
                 InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isMatches(String inputPassword, User user) {
        String hashedInputPassword = encrypt(user.getUserId(), inputPassword);
        return hashedInputPassword.equals(user.getPassword());
    }

    private byte[] getSalt(String userId)
            throws NoSuchAlgorithmException, UnsupportedEncodingException {

        MessageDigest digest = MessageDigest.getInstance("SHA-512");
        byte[] keyBytes = userId.getBytes("UTF-8");

        return digest.digest(keyBytes);
    }
}