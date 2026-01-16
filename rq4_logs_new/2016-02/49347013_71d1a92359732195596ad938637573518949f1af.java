package assigning;

/**
 * Created by leandro on 27-2-2016.
 */
public class AssignmentTest {
    public static void main(String[] args) {
        String t;
        String s = t = "hoi";
        System.out.println(s);

        double a = 85.62;
        System.out.printf("%,+6.2f%n", a);
        System.out.printf("%+,3f\n", a);
        System.out.printf("%+,20f\n", a);
        System.out.printf("%+,f\n", a);
        System.out.printf("%,+f\n", a);
}
}