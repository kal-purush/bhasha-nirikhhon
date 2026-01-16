import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class password {


        public static void main(String args[]) throws NoSuchAlgorithmException
        {
            String s = "Balu";
           String e="";
           for(int i=0;i<s.length();i++){
               e+=(char)((int)s.charAt(i)-1);
           }
            System.out.println(e);
           String m="";
            for(int i=0;i<e.length();i++){
                m+=(char)((int)e.charAt(i)+1);
            }
            System.out.println(m);
        }
}