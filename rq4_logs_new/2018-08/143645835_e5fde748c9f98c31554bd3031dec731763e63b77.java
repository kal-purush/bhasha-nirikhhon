import java.util.HashMap;
import java.util.Map;
import java.util.*;

public class RansomNote {

    public static void main(String[] args){
        String[] magazine = new String[] {"two", "times", "three", "is", "not", "four"};
        String[] note = new String[] {"two", "times", "two", "is", "four"};
        checkMagazine(magazine, note);
    }
    // Complete the checkMagazine function below.
    static void checkMagazine(String[] magazine, String[] note) {
        String [] used_keys = new String [magazine.length];
        int valid_words = 0; 
        boolean words_match = true;
        Map<String, Integer> magazineValues = new HashMap<String, Integer>();
        
        //Add values to hashmap
        for(int i = 0; i <magazine.length; i++){
            magazineValues.put(magazine[i], i);
        }
        
        while(words_match){
            for(int i = 0; i < note.length; i++){
                if(magazineValues.containsKey(note[i])){
                    if(!(Arrays.asList(used_keys).contains(note[i]))){
                        //!(Arrays.asList(used_keys).contains(magazineValues.get(note[i])))
                        valid_words++;
                        used_keys[i] = note[i];
                        
                    }
                }
                else{
                    words_match = false;
                }
                if(i == (note.length - 1)){
                    words_match = false;
                    
                }
            }
        }
        if(valid_words == note.length){
            System.out.println("Yes");
        }
        else{
            System.out.println("No");
        }
    }
}