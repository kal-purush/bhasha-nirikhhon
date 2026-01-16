import java.math.*;

public class PublicKey
{
   private BigInteger privateExponent;
   private BigInteger publicExponent;
   private BigInteger modulus;

   public PublicKey()
   {
      privateExponent = new BigInteger("17");
      publicExponent = new BigInteger("2753");
      modulus = new BigInteger("3233");
   }
   
   public PublicKey(String publicExponent1, String modulus1)
   {
      publicExponent = new BigInteger(publicExponent1);
      modulus = new BigInteger(modulus1);
   }
   
   public BigInteger getPublicExponent()
   {
      return publicExponent;
   }
   
   public BigInteger getModulus()
   {
      return modulus;
   }
   
   public BigInteger encryptChar(char inMessage)
   {
      String mString = (int) inMessage + "";
      BigInteger m = new BigInteger(mString);
      BigInteger encrypted = m.modPow(publicExponent, modulus);
      return encrypted;
   }
   
   public char decryptChar(BigInteger cipherMessage)
   {
      BigInteger m = cipherMessage.modPow(privateExponent, modulus);
      char decrypted = (char) m.intValue();
      return decrypted;
   }
   
}