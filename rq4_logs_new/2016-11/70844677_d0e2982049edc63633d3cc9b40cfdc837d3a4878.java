package com.exigenservices.lectures;

import javax.ejb.Remote;
import javax.ejb.Stateless;
import java.util.HashMap;
import java.util.Map;

@Remote(CharCountRemote.class)
@Stateless(mappedName = "CharCountBean")
public class CharCountBean implements CharCountRemote{

    @Override
    public Map<Character, Integer> countLetterAmounts(String text) {
        HashMap<Character, Integer> queryResult = new HashMap<Character, Integer>();
        for(int i = 0; i < text.length(); i++) {
            Integer charOccurences = queryResult.get(text.charAt(i));
            if(charOccurences == null) {
                queryResult.put(text.charAt(i), 1);
            } else {
                queryResult.put(text.charAt(i), charOccurences + 1);
            }
        }
        return queryResult;
    }

    @Override
    public String respond() {
        return "nya!";
    }
}