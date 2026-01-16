package application;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Base64;

public class EncryptionUtils {
    private static final String ENCRYPTION_ALGORITHM = "AES";

    // Replace this with your actual passphrase
    private static final String PASSPHRASE = "YourPassphrase1234";

    private static byte[] deriveKey(String passphrase, byte[] salt) throws NoSuchAlgorithmException, InvalidKeySpecException {
        int iterations = 10000;
        int keyLength = 128; // AES-128
        KeySpec spec = new PBEKeySpec(passphrase.toCharArray(), salt, iterations, keyLength);
        SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        return factory.generateSecret(spec).getEncoded();
    }

    public static String encrypt(String input) throws Exception {
        byte[] salt = new byte[16];
        // Generate a random salt
        // Note: In a real-world scenario, you should use a secure random generator
        // to create a salt, and store the salt securely along with the encrypted data.
        for (int i = 0; i < 16; i++) {
            salt[i] = (byte) i; // Use a real random salt in production
        }

        byte[] key = deriveKey(PASSPHRASE, salt);
        SecretKeySpec secretKey = new SecretKeySpec(key, ENCRYPTION_ALGORITHM);

        Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);
        byte[] encryptedBytes = cipher.doFinal(input.getBytes());
        return Base64.getEncoder().encodeToString(encryptedBytes);
    }

    public static String decrypt(String encryptedInput) throws Exception {
        byte[] salt = new byte[16];
        // Generate the same salt that was used during encryption.
        for (int i = 0; i < 16; i++) {
            salt[i] = (byte) i; // Use the same salt used in encryption
        }

        byte[] key = deriveKey(PASSPHRASE, salt);
        SecretKeySpec secretKey = new SecretKeySpec(key, ENCRYPTION_ALGORITHM);

        Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, secretKey);
        byte[] encryptedBytes = Base64.getDecoder().decode(encryptedInput);
        byte[] decryptedBytes = cipher.doFinal(encryptedBytes);
        return new String(decryptedBytes);
        
    }

    public static void main(String[] args) {
        String originalString = "L3cr3X2bNCtcvf";

        try {
            // Encryption
            String encryptedString = encrypt(originalString);
            System.out.println("Original String: " + originalString);
            System.out.println("Encrypted String: " + encryptedString);
            
           
            // Decryption
            String decryptedString = decrypt(encryptedString);
            System.out.println("Decrypted String: " + decryptedString);
            
            if(encryptedString == "H+WZuEtWjBChNUyoMTGf2QtGvNlO9GH8h/6Vz4rPjdY=") {
            	System.out.println("equal");
            } else {
            	System.out.print("Not");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}







