
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CommonTest {

    public static <E> E getTheFirst(List<E> l) {
        System.out.println("invoke method(List<E> l)");
        if(l != null && !l.isEmpty()) {
            E result = l.get(0);
            return result;
        }else{
            return null;
        }
    }

    public void conditionTest() {
        if(true) {
            System.out.println("true");
        }else{
            System.out.println("false");
        }
    }

    public static void main(String[] args) {
        Integer a = 1;
        Integer b = 2;
        Integer c = 3;
        Long g = 3L;
        System.out.println(c == (a + b));
        System.out.println(g == (a + b));
        System.out.println(c.equals(a + b));
        System.out.println(g.equals(a + b));

        /*System.out.println(CommonTest.getTheFirst(Arrays.asList("A", "B", "C")));
        System.out.println(CommonTest.getTheFirst(Arrays.asList(11, 22, 33)));*/
    }
}