import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Objects;
import javax.xml.bind.DatatypeConverter;

public class Cracker {

    public static String crackSha256(String h, String chars) {
        // if the string is lower than 10 chars, it generates 9!=362880 possibilities at most.
        ArrayList<String> variations = permute(chars);
        System.out.println("Number of permuted elements="+variations.size());
        for (String var : variations) {
            if (Objects.equals(generateSHA256(var), h)) return var;
        }
        return null;
    }

    private static ArrayList<String> permute(String chars) {
        ArrayList<String> variations = new ArrayList<>();
        if (chars.length() == 1) {
            variations.add(chars);
            return variations;
        }
        for (int i = 0; i < chars.length(); i++) {
            String c = "";
            for (int j = 0; j < chars.length(); j++) {
                if (i == j) continue;
                c += chars.charAt(j);
            }
            ArrayList<String> pet = permute(c);
            for (String s : pet) {
                variations.add(chars.charAt(i)+s);
            }
        }
        return variations;
    }

    private static String generateSHA256 (String str) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(str.getBytes("UTF-8"));
            String theHash = DatatypeConverter.printHexBinary(hash);
            return theHash.toLowerCase();
        }catch(Exception e){
            return null;
        }
    }
}