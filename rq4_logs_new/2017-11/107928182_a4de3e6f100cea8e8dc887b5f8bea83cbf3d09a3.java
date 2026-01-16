package org.apframework.inverted;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * From http://eriol.iteye.com/blog/1166989
 */
public class InvertedIndex {

    private Map<String, Map<Integer, Integer>> index;

    public void createIndex(String filePath) {
        index = new HashMap<>();

        try {
            InputStream is = new FileInputStream(ClassLoader.getSystemResource(filePath).getFile());
            BufferedReader read = new BufferedReader(new InputStreamReader(is));

            String temp;
            int line = 1;
            Map<Integer, Integer> subIndex;
            while ((temp = read.readLine()) != null) {
                String[] words = temp.split(" ");
                for (String word : words) {
                    if (!index.containsKey(word)) {
                        subIndex = new HashMap<>();
                        subIndex.put(line, 1);
                        index.put(word, subIndex);
                    } else {
                        subIndex = index.get(word);
                        if (subIndex.containsKey(line)) {
                            int count = subIndex.get(line);
                            subIndex.put(line, count + 1);
                        } else {
                            subIndex.put(line, 1);
                        }
                    }
                }
                line++;
            }
            read.close();
            is.close();
        } catch (IOException e) {
            System.out.println("error in read file");
        }
    }

    public void find(String str) {
        String[] words = str.split(" ");
        for (String word : words) {
            StringBuilder sb = new StringBuilder();
            if (index.containsKey(word)) {
                sb.append("word: " + word + " in ");
                Map<Integer, Integer> temp = index.get(word);
                for (Map.Entry<Integer, Integer> e : temp.entrySet()) {
                    sb.append("line " + e.getKey() + " [" + e.getValue() + "] , ");
                }
            } else {
                sb.append("word: " + word + " not found");
            }
            System.out.println(sb);
        }
    }

    public static void main(String[] args) {
        InvertedIndex index = new InvertedIndex();
        index.createIndex("source-file");
        index.find("banana");
    }
}