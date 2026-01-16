package GroupStudy;
import com.sun.security.jgss.GSSUtil;
import java.util.HashMap;

public class LearnHashMap {
    public static void main(String[] args) {
//        int a = 10;
//        int b = 3;
//        int c = 30;
//
//        HashMap<String, Integer> hash = new HashMap<String, Integer>();
//
//        hash.put("a", 90);
//        hash.put("b", 20);
//        hash.put("c", 40);
//        System.out.println(hash);
//        System.out.println(hash.get("c"));
        HashMap<String, String> hsm = new HashMap<String, String>();

        hsm.put("gmail", "1243242");
        hsm.put("facebook", "what");
        hsm.put("insta", "whay");


        //hsm.remove("gmail");//to remove
        System.out.println(hsm);//print all the hashmap

        System.out.println(hsm.containsValue("what"));// to see if a password is in there
        System.out.println(hsm.containsKey("gmail"));//to check keys in there
        System.out.println(hsm.size());//to see size

        System.out.println(hsm.replace("gmail", "love"));//to change a value for a key
        System.out.println(hsm.keySet());//to see whats in the key

        for (String i:hsm.keySet()) {
            System.out.println("keys are:"+ i+" " +
                    "values are :" + hsm.get(i));

        }}
}