/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package analyzertfidf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

/**
 *
 * @author Bryan
 */
public class TextProcessor {

    ArrayList<String> mostCommonWords;

    public TextProcessor() {
        readCommonWords();
    }

    private void readCommonWords() {
        mostCommonWords = new ArrayList<>();

        try {
            FileReader fr = new FileReader(new File("100MostUsedWords.txt"));
            BufferedReader br = new BufferedReader(fr);
            String line = "";

            while ((line = br.readLine()) != null) {
                mostCommonWords.add(line.toLowerCase());
            }

        } catch (FileNotFoundException ex) {
            throw new Error(ex.getMessage());
        } catch (IOException ex) {
            throw new Error(ex.getMessage());
        }
    }

    public LinkedHashMap readFile(String fileName) {
        
        // Read file
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            
            return findKeywords(reader);
            
        } catch (Exception e) {
            throw new Error("Reading File Exception", e);
        }
    }

    public LinkedHashMap findKeywords(BufferedReader br) throws IOException {

        HashMap<String, Integer> tempMap = new HashMap<>();
        String line = "";
        
        // Split symbols
        while ((line = br.readLine()) != null) {            
            String[] wordArray = line.toLowerCase().split("[\\p{Punct}\\s]+");
            
            // Generate HashMap with keys and values
            for (String word : wordArray) {
                // Remove letters
                if (word.length() > 1) {
                    // Remove most common words
                    if (!mostCommonWords.contains(word)) {
                        // Increment if word is known
                        if (tempMap.containsKey(word)) {
                            tempMap.put(word, tempMap.get(word) + 1);
                        } else {
                            // Add new word and frequency to map
                            tempMap.put(word, 1);
                        }
                    }
                }
            }
        }
        
        // Sort HashMap and return a LinkedHashMap
        return sortHashMapByValuesD(tempMap);
    }

    /**
     * http://stackoverflow.com/questions/8119366/sorting-hashmap-by-values
     * 
     * @param passedMap Map to sort
     * @return 
     */
    public LinkedHashMap sortHashMapByValuesD(HashMap passedMap) {
        List mapKeys = new ArrayList(passedMap.keySet());
        List mapValues = new ArrayList(passedMap.values());
        Collections.sort(mapValues);
        Collections.sort(mapKeys);

        LinkedHashMap sortedMap = new LinkedHashMap();

        Iterator valueIt = mapValues.iterator();
        while (valueIt.hasNext()) {
            Object val = valueIt.next();
            Iterator keyIt = mapKeys.iterator();

            while (keyIt.hasNext()) {
                Object key = keyIt.next();
                String comp1 = passedMap.get(key).toString();
                String comp2 = val.toString();

                if (comp1.equals(comp2)) {
                    passedMap.remove(key);
                    mapKeys.remove(key);
                    sortedMap.put((String) key, (Integer) val);
                    break;
                }
            }
        }
        return sortedMap;
    }

}