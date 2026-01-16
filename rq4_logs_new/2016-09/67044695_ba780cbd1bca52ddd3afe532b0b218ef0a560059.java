package CS3213;

import java.util.Arrays;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Created by Zi Xian on 04/09/2016.
 */
public class RequiredWords {
    private TreeSet<String> reqWords;

    public RequiredWords(String[] reqWords){
        // This project doesn't seem to use JDK 1.8 so lambda expressions not supported :(
        this.reqWords = new TreeSet<String>();
        for(String word: reqWords){
            this.reqWords.add(word);
        }
    }

    /**
     * Filters out given lines based on its keyword.
     * A line passes if keyword matches any of its filter words or if there is no fitler word.
     * @param lines The lines to be filtered
     * @return Array of strings that pass this filter requirements
     */
    public String[] filter(String[] lines){
        return lines;
    }
}