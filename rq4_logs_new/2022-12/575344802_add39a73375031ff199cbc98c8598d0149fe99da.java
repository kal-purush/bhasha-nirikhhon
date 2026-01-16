import java.util.HashMap;
import java.util.Map;

public class WordsChecker {
    private String text;
    private Map<String, Integer> map = new HashMap<String, Integer>();
    private Integer i = 0;
    private final Integer initMap = 1;

    public WordsChecker(String text){
        this.text = text;
    }

    public boolean textToMap(){
        for (String word : text.split("\\P{IsAlphabetic}+")) {

            if (map.containsKey(word)) {
                i = map.get(word) + 1;
                map.put(word, i);
            } else {
                map.put(word, initMap);
            }
        }
        return true;
    }

    public void getShow(){
//        textToMap();
        System.out.println(map);
    }

    public boolean search(String txtSearch){
        //        textToMap()
        if (map.containsKey(txtSearch)){
            System.out.println("the word " + txtSearch + " occurs " + map.get(txtSearch) + " times");
            return true;
        }
        return false;
    }
}