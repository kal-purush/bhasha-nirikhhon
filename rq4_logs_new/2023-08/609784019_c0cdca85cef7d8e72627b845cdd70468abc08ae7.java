package application;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class DataEncryptorDecryptor {
    public static String encrypt(String data, String encryptionKey) throws Exception {
        SecretKeySpec keySpec = new SecretKeySpec(encryptionKey.getBytes("UTF-8"), "AES");
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.ENCRYPT_MODE, keySpec);
        return Base64.getEncoder().encodeToString(cipher.doFinal(data.getBytes("UTF-8")));
    }

	private static final String ENCRYPTION_ALGORITHM = "AES";
	    
	    public static String decrypt(String encryptedData, String encryptionKey) {
	        try {
	            // Decode the Base64-encoded data
	            byte[] decodedData = Base64.getDecoder().decode(encryptedData);
	
	            // Decrypt the data using the encryption key
	            Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM);
	            SecretKeySpec keySpec = new SecretKeySpec(encryptionKey.getBytes(), ENCRYPTION_ALGORITHM);
	            cipher.init(Cipher.DECRYPT_MODE, keySpec);
	            byte[] decryptedBytes = cipher.doFinal(decodedData);
	
	            // Convert the decrypted bytes to a string
	            return new String(decryptedBytes, StandardCharsets.UTF_8);
	        } catch (Exception e) {
	            e.printStackTrace();
	            return null; // Handle decryption failure gracefully in your application
	        }
	    }

    public static String generateAESKey() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(128); // 128-bit key size
        SecretKey secretKey = keyGenerator.generateKey();
        byte[] encodedKey = secretKey.getEncoded();
        return Base64.getEncoder().encodeToString(encodedKey);
    }
}