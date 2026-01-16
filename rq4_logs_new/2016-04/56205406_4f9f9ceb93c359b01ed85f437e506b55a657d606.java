package com.company;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

public class WordsFreq {
        public void run() throws IOException {
            String path = "sample.txt";
            String text = readFile(path);
            text = cleanText(text);
            text = text.toLowerCase();
            StringTokenizer stringTokenizer = new StringTokenizer(text, " \n");
            HashMap<String, Integer> freqMap = new HashMap<String, Integer>();
            while (stringTokenizer.hasMoreTokens()) {
                String word = stringTokenizer.nextToken();
                if (!freqMap.containsKey(word)) {
                    freqMap.put(word, 0);
                }
                freqMap.put(word, freqMap.get(word) + 1);
            }
            StringBuilder result = new StringBuilder();

            freqMap = (HashMap<String, Integer>) sortByValue(freqMap);
            for (String word : freqMap.keySet()) {
                result.append(word);
                result.append(" " + freqMap.get(word) + "\n");
            }

            FileWriter out = new FileWriter("wordsFreq.txt");

            out.write(result.toString());
            out.flush();
            out.close();


        }

        public  Map<String, Integer>
        sortByValue(Map<String, Integer> map) {
            List<Map.Entry<String, Integer>> list =
                    new LinkedList<Map.Entry<String, Integer>>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return (o2.getValue()).compareTo(o1.getValue());
                }
            });

            Map<String, Integer> result = new LinkedHashMap<String, Integer>();
            for (Map.Entry<String, Integer> entry : list) {
                result.put(entry.getKey(), entry.getValue());
            }
            return result;
        }

        public String readFile(String name) throws IOException {
            File file = new File(name);
            return new String(Files.readAllBytes(file.toPath()));
        }

        public String cleanText(String text) {
            String ans = "";

            for (int i = 0; i < text.length(); i++) {
                if (!Character.isISOControl(text.charAt(i))) {
                    ans += text.charAt(i);
                }
            }

            return ans;
        }


    }
