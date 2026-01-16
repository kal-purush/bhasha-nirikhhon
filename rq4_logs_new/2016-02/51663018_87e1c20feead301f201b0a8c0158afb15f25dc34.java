import java.math.*;

public class CipherTest
{
   public static void main(String[] arg)
   {
      PublicKey encryptKey = new PublicKey("2753", "3233");
      PublicKey decryptKey = new PublicKey();
      
      String inString = "Eli Fonseca";
      BigInteger[] cipherString = new BigInteger[inString.length()];
      
      for (int ii = 0; ii < inString.length(); ii++)
      {
         cipherString[ii] = encryptKey.encryptChar(inString.charAt(ii));
      }
      
      String decryptString = "";
      
      for (int ii = 0; ii < cipherString.length; ii++)
      {
         decryptString += decryptKey.decryptChar(cipherString[ii]);
      }
      
      System.out.println(inString);
      System.out.println(decryptString);
      
      /*
      BigInteger cipherMessage = encryptKey.encryptChar(' ');
      char plainMessage = decryptKey.decryptChar(cipherMessage);
      System.out.println("a" + inMessage + "a");
      System.out.println(outKey.getPublicExponent().toString());
      System.out.println(outKey.getModulus().toString());
      */
   }

}