package Hash_Table;

import java.util.*;
import java.util.HashMap;

/**
 * Created by Nick on 3/27/17.
 */
public class Hash {

    public static void main(String[] args){
        System.out.println(Solution("Ask not what your country can do for you but what you can do for your country", "Ask Ask"));

    }

//    Map<String, Integer> magazineMap;
//    boolean isValid;

    public static boolean Solution(String magazine, String note) {

        java.util.HashMap<String, Integer> magazineMap = new HashMap<>();

        for (String word : magazine.split(" ")) {
            Integer i = magazineMap.get(word);

            if (i == null) {
                magazineMap.put(word, 1);
            } else {
                magazineMap.put(word, i + 1);
            }
        }

        for (String word : note.split(" ")) {
            Integer i = magazineMap.get(word);

            if (i == null || magazineMap.get(word) == 0) {
                return false;

            } else {
                magazineMap.put(word, i - 1);
            }
        }
        return true;
    }


}