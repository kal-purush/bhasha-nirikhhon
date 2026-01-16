package akylai.map2;

import java.util.HashMap;
import java.util.Map;

public class FirstChar {
    public Map<String, String> firstChar(String[] strings) {
        Map<String, String> newMap = new HashMap<>();
        for (String word : strings){
            if (!word.isEmpty()){
                String firstChar = String.valueOf(word.charAt(0));
                if (newMap.containsKey(firstChar)){
                    newMap.put(firstChar, newMap.get(firstChar) + word);
                }
                else {
                    newMap.put(firstChar, word);
                }
            }
        }
        return newMap;
    }
}