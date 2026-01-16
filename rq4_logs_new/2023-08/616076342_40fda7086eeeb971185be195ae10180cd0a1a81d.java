import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;

public class Exercise5 {
    public static void main(String[] args) {
        HashSet<Object> set = new HashSet<>();

        set.add(10);
        set.add(20);

        set.add(new String("sang"));
        set.add(new String("pham"));

        set.add(new MyOwnClass());
        set.add(new MyOwnClass());

        Iterator it = set.iterator();

        while (it.hasNext()) {

            // Print HashSet values
            System.out.print(it.next() + " ");
        }

        System.out.println();

        for (Object a:set){
            System.out.printf(a+" ");
        }

        String[] a = new String[3];
        a[1] = new String("SANG");
        a[2] = new String("SANG");
        a[3] = new String("PHAM");

        HashSet<String[]> set1= new HashSet<>();

    }
}