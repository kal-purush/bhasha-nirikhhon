package com.example.justpoteito.security;

import java.io.File;
import java.nio.file.Files;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;

import javax.crypto.Cipher;

public class RsaEncrypter {

    private static final String PUBLIC_KEY_FILE_PATH = "./public.key";

    public static byte[] encryptText(String mensaje) {
        byte[] encodedMessage = null;

        try {
            File ficheroPublica = new File(PUBLIC_KEY_FILE_PATH);
            byte[] clavePublica = new byte[0];

            if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.O) {
                clavePublica = Files.readAllBytes(ficheroPublica.toPath());
            }

            System.out.println("Tamanio -> " + clavePublica.length + " bytes");

            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            X509EncodedKeySpec x509EncodedKeySpec = new X509EncodedKeySpec(clavePublica);
            PublicKey publicKey = keyFactory.generatePublic(x509EncodedKeySpec);

            Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
            cipher.init(Cipher.ENCRYPT_MODE, publicKey);
            encodedMessage = cipher.doFinal(mensaje.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return encodedMessage;
    }
}