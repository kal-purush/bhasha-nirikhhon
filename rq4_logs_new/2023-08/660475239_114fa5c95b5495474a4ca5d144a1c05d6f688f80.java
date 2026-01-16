package day18varargsstringbuilderbuffer;

public class StringBuilder02 {
    public static void main(String[] args) {

        StringBuilder s = new StringBuilder();
        s.append("Java");

        int x = s.length();
        int y = s.capacity();
        System.out.println(x);
        System.out.println(y);

        StringBuilder s1 = new StringBuilder();
        s1.append("Ja");

        int z = s1.length();
        int t = s1.capacity();
        System.out.println(z);
        System.out.println(t);

        StringBuilder s2 = new StringBuilder(3);
        s2.append("abc");


    }
}