package Akylai.Map1;

import java.util.Map;

public class MapAB4 {
    public Map<String, String> mapAB4(Map<String, String> map) {
        if (map.containsKey("a") && map.containsKey("b")) {
            String a = map.get("a");
            String b = map.get("b");
            if (a != null && b != null) {
                if (a.length() > b.length()) {
                    map.put("c", a);
                } else if (a.length() < b.length()) {
                    map.put("c", b);
                } else {
                    map.put("a", "");
                    map.put("b", "");
                }
            }
        }
        return map;
    }
}