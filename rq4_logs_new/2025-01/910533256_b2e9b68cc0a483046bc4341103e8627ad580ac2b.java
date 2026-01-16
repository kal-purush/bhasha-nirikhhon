package tictactoe.domain.usecases;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashingUseCase {

    public static String hashPassword(String password) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = md.digest(password.getBytes());

            String hash = "";
            for (byte b : hashedBytes) {
                hash += String.format("%02x", b); 
            }

            return hash.toString();

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not found!", e);
        }
    }
}