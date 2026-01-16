import java.util.HashSet;
import java.util.*;


public class NewCollection {
    public Map<String, Integer> map2 = new HashMap<String, Integer>();

    String key;
    int val;

    public NewCollection(String key, int val) {
        this.key = key;
        this.val = val;
    }

    public void setInt(String key, Integer value) {
        if (!map2.containsKey(key)) {
            map2.put(key, value);
        } else
           if (map2.containsKey(key) && map2.containsValue(value)) {
           throw new IllegalArgumentException("Значения совпадают");
        } else

           if (map2.containsValue(key)){
              map2.put(key, 5);
           }

    }


    public String getKey() {
        return key;
    }

    public int getVal() {
        return val;
    }

    @Override
    public String toString() {
        return "NewCollection{" +
                "key='" + key + '\'' +
                ", val=" + val +
                '}';
    }
}