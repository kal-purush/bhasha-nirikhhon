package rish.leets.grind.easy;

import java.util.ArrayList;
import java.util.List;

public class ValidAnagram {

    public boolean isAnagram(String s, String t) {
        
        if (s.length() != t.length()) {
            return false;
        }

        List<Character> charSet = new ArrayList<>();
        for (int i = 0; i < s.length(); i++) {
          charSet.add(Character.valueOf(s.charAt(i)));
        }

        for (int i = 0; i < t.length(); i++) {
            Character temp = Character.valueOf(t.charAt(i));
            if (!charSet.contains(temp)) {
                return false;
            }
            charSet.remove(temp);
        }

        return true;
    }

}