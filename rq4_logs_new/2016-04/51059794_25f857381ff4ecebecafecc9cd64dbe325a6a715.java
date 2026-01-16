package com.ikode.viezara.ikode;

/**
 * Created by viezara on 13/04/2016.
 */
import android.util.Base64;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;

import javax.crypto.Cipher;

public class RsaCryptography {

    private final String ALGORITHM = "RSA/None/OAEPWithSHA1AndMGF1Padding";
    private final String PROVIDER = "BC";
    private String pk = "";

    RsaCryptography(String pk) {
        this.pk = pk;
    }

    public String encryptMessage(String message) throws Exception {
        String container = "";

        // create the key factory
        KeyFactory kFactory = KeyFactory.getInstance("RSA", new BouncyCastleProvider());
        // decode base64 of your key
        byte yourKey[] =  Base64.decode(pk, Base64.DEFAULT);
        //Log.v("test", new String(yourKey));

        // generate the public key
        X509EncodedKeySpec spec =  new X509EncodedKeySpec(yourKey);
        PublicKey publicKey = (PublicKey) kFactory.generatePublic(spec);

        // now you can for example cipher some data with your key
        Cipher cipher = Cipher.getInstance(ALGORITHM, PROVIDER);
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);
        byte[] cipherData = org.apache.commons.codec.binary.Base64.encodeBase64(cipher.doFinal(message.getBytes()));

        container = new String(cipherData);

        return container;
    }

    public String decryptMessage(String encodedMessage) throws Exception {
        String container = "";

        byte[] byteMessage = Base64.decode(encodedMessage, Base64.DEFAULT);

        // create the key factory
        KeyFactory kFactory = KeyFactory.getInstance("RSA", new BouncyCastleProvider());
        // decode base64 of your key
        byte yourKey[] =  Base64.decode(pk, Base64.DEFAULT);
        //Log.v("test", new String(yourKey));

        // generate the public key
        X509EncodedKeySpec spec =  new X509EncodedKeySpec(yourKey);
        PublicKey publicKey = (PublicKey) kFactory.generatePublic(spec);

        // now you can for example cipher some data with your key
        Cipher cipher = Cipher.getInstance(ALGORITHM, PROVIDER);
        cipher.init(Cipher.DECRYPT_MODE, publicKey);
        byte[] decryptedMessage = cipher.doFinal(byteMessage);

        container = new String(decryptedMessage, "UTF-8");

        return container;
    }
}