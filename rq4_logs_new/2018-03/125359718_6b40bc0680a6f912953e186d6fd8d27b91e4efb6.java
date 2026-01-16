import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by andriusbaltrunas on 3/14/2018.
 */
public class ListExample {
    public static void main(String[] args) {

        List<String> names = new ArrayList<>();

        names.add("Andrius");
        names.add("Tomas");
        names.add("Jonas");
        names.add("Antanas");
        names.add("Jokubas");
        Collections.sort(names);
        for(String name: names){
            System.out.println(name);
        }

        System.out.println(names.get(3));

        System.out.println(names.size());

        System.out.println(names.isEmpty());

        System.out.println(names.contains("Jonas"));

    }
}