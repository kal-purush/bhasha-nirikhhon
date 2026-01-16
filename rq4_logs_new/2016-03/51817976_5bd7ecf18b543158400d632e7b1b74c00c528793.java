/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package analyzertfidf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author Bryan
 */
public class Corpus {

    ArrayList<Text> texts;
    Keywords keywords;

    public Corpus() {
        texts = new ArrayList<>();
        keywords = new Keywords(new HashMap<>());
    }

    public Corpus(Text text) {
        texts = new ArrayList<Text>();
        keywords = new Keywords(text.keywords.keywordMap);
        update(text);
    }

    public Corpus(ArrayList<Text> texts) {
        this.texts = new ArrayList<>();
        keywords = new Keywords(new HashMap<>());

        for (Text t : texts) {
            update(t);
        }
    }

    public void update(Text text) {
        // TODO update keywords
        // Add text to corpus
        texts.add(text);

        // compare text keywords with the rest of the corpus texts
        for (Text t : texts) {

            Iterator it = t.keywords.keywordMap.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();

                if (keywords.keywordMap.containsKey(pair.getKey())) {
                    keywords.keywordMap.put(pair.getKey().toString(), keywords.keywordMap.get(pair.getKey()) + 1);
                } else {
                    keywords.keywordMap.put(pair.getKey().toString(), 1);
                }

            }

        }
    }

    @Override
    public String toString() {
        return keywords.toString();
    }
}