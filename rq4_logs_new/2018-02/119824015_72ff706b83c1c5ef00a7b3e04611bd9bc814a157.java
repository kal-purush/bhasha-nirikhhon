package com.sibentek.bankapp.domain.convert;

import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.codec.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import java.security.Key;
import java.util.Properties;

/**
 * Created by william on 20/02/2018.
 */
@Slf4j
@Converter
public class JPACryptoConverter implements AttributeConverter<String, String> {

    private static String ALGORITHM = null;
    private static byte[] KEY = null;

    public static final String algorithm_property_key = "encryption.algorithm";
    public static final String secret_property_key = "encryption.key";

    static final Properties properties = new Properties();

    static {
        try {
            properties.load(JPACryptoConverter.class.getClassLoader()
                    .getResourceAsStream("persistence.properties"));
        } catch (Exception e) {
            log.warn("Could not load properties file 'persistence.properties' using unsecure encryption key.");
            properties.put(algorithm_property_key, "AES/ECB/PKCS5Padding");
            properties.put(secret_property_key, "MySuperSecretKey");
        }
        ALGORITHM = (String) properties.get(algorithm_property_key);
        KEY = ((String) properties.get(secret_property_key)).getBytes();
    }

    @Override
    public String convertToDatabaseColumn(String sensitive) {
        Key key = new SecretKeySpec(KEY, "AES");
        try {
            final Cipher c = Cipher.getInstance(ALGORITHM);
            c.init(Cipher.ENCRYPT_MODE, key);
            final String encrypted = new String(Base64.encode(c
                    .doFinal(sensitive.getBytes())), "UTF-8");
            return encrypted;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String convertToEntityAttribute(String sensitive) {
        Key key = new SecretKeySpec(KEY, "AES");
        try {
            final Cipher c = Cipher.getInstance(ALGORITHM);
            c.init(Cipher.DECRYPT_MODE, key);
            final String decrypted = new String(c.doFinal(Base64
                    .decode(sensitive.getBytes("UTF-8"))));
            return decrypted;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}