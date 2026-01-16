import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
public class UseMap {
    public static void main(String[] args) {

        Map<String, Integer> credintials = new HashMap<String, Integer>();
        credintials.put("almrashid",123455);
        credintials.put("jayMathur",223333);
        credintials.put("JhonSmith",333322);

        for(Map.Entry<String, Integer> cd:credintials.entrySet()){
            System.out.println(cd.getKey() + "---> " + cd.getValue());
        }
    }
}