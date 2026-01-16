package pl.dgrebowiec.counterwords.provider;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Dariusz Grebowiec <dariusz.grebowiec@coi.gov.pl>
 */

@Component
public class CounterWordsProvider {

    private List<String> getWords(String text) {
        List<String> words = new ArrayList<>();
        if(text == null) return words;

        Pattern pattern = Pattern.compile("[a-z]+");
        Matcher matcher = pattern.matcher(text.toLowerCase());
        while (matcher.find()) {
            words.add(matcher.group());
        }
        return words;
    }

    private Map<String, Integer> getGroupAndSortedWords(List<String> words) {
        Map<String, Integer> groupWords = new TreeMap<>();
        for (String word : words) {
            if (groupWords.containsKey(word)) {
                groupWords.put(word, groupWords.get(word) + 1);
            } else {
                groupWords.put(word, 1);
            }
        }
      /* groupWords = groupWords.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getValue))
               .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
               */

        return sortByValue(groupWords);
    }

    public Map<String, Integer> counterWords(String text) {
        return getGroupAndSortedWords(getWords(text));
    }

    private <K, V extends Comparable<? super V>> Map<K, V>
    sortByValue( Map<K, V> map )
    {
        Map<K,V> result = new LinkedHashMap<>();
        Stream<Map.Entry<K,V>> st = map.entrySet().stream();

        st.sorted(Comparator.comparing(e -> e.getValue()))
                .forEachOrdered(e ->result.put(e.getKey(),e.getValue()));

        return result;
    }
}

