package Hashing;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.SecretKeyFactory;
// import javax.crypto.interfaces.PBEKey;
import javax.crypto.spec.PBEKeySpec;

public class Hashing {
//  public static void main(String[] args) throws NoSuchAlgorithmException, InvalidKeySpecException {
//     // String pass = "Matkhua01";
//     System.out.println(generatePasswordHasing("matkhau01"));
//  }
    public String Hashing (String a) throws NoSuchAlgorithmException, InvalidKeySpecException{
      
      return  generatePasswordHasing(a);
     }
    private static String generatePasswordHasing(String password) throws NoSuchAlgorithmException, InvalidKeySpecException {
        char[] passChar = password.toCharArray();
        byte[] salt = getSalt();
        PBEKeySpec keysSpec = new PBEKeySpec(passChar, salt, 1000,64*8);
        SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
        byte[] hash= skf.generateSecret(keysSpec).getEncoded();   
        return "1000"+":"+ toHex(salt)+":" + toHex(hash);

    }
    private static byte[] getSalt()throws NoSuchAlgorithmException{
        SecureRandom sr =  SecureRandom.getInstance("SHA1PRNG");
        byte[] salt = new byte[16];
        sr.nextBytes(salt);
        // System.out.println( );
        return salt;
    }
    private static String toHex(byte[] b) throws NoSuchAlgorithmException{
        BigInteger bi = new BigInteger(1,b);
        String hex = bi.toString(16);
        int paddingLength = (b.length*2)-hex.length();
        if (paddingLength>0){
            return String.format("%0" + paddingLength+"d",0)+hex;
        } else return hex;
        
    }
}