package desencryptedchat;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

public class Message {

    private String sentPlainText;
    private byte[] sentCipherText;
    private String receivedPlainText;
    private byte[] receivedCipherText;

    public Message() {}

    public String getSentPlainText() {
        return sentPlainText;
    }

    public byte[] getSentCipherText() {
        return sentCipherText;
    }

    public void setSentPlainText(String sentPlainText) {
        this.sentPlainText = sentPlainText;
    }

    public void setSentCipherText(byte[] sentCipherText) {
        this.sentCipherText = sentCipherText;
    }

    public String getReceivedPlainText() {
        return receivedPlainText;
    }

    public byte[] getReceivedCipherText() {
        return receivedCipherText;
    }

    public void setReceivedPlainText(String receivedPlainText) {
        this.receivedPlainText = receivedPlainText;
    }

    public void setReceivedCipherText(byte[] receivedCipherText) {
        this.receivedCipherText = receivedCipherText;
    }

    public void encryptMessage(SentMessage sentMessage, SecretKey originalKey, Cipher desCipher) throws NoSuchAlgorithmException,
            NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
        desCipher.init(Cipher.ENCRYPT_MODE, originalKey);
        byte[] text = sentMessage.getSentPlainText().getBytes();
        byte[] textEncrypted = desCipher.doFinal(text);
        sentMessage.setSentCipherText(textEncrypted);
    }

    public void decryptMessage(ReceivedMessage receivedMessage, SecretKey originalKey, Cipher desCipher) throws NoSuchAlgorithmException,
    NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
        desCipher.init(Cipher.DECRYPT_MODE, originalKey);
        byte[] text = receivedMessage.getReceivedCipherText();
        byte[] textDecrypted = desCipher.doFinal(text);
        receivedMessage.setReceivedPlainText(new String(textDecrypted));
    }

    public void displaySentData(SentMessage sentMessage, SecretKey originalKey) {
        System.out.println("Key: " + Base64.getEncoder().encodeToString(originalKey.getEncoded()));
        System.out.println("Original Plain Text: " + sentMessage.getSentPlainText());
        System.out.println("Sent Cipher Text: " + new String(sentMessage.getSentCipherText()));
    }

    public void displayReceivedData(ReceivedMessage receivedMessage, SecretKey originalKey) {
        System.out.println("Key: " + Base64.getEncoder().encodeToString(originalKey.getEncoded()));
        System.out.println("Received Cipher Text: " + new String(receivedMessage.getReceivedCipherText()));
        System.out.println("Received Plain Text: " + receivedMessage.getReceivedPlainText());
    }
}