package day18varargsstringbuilderbuffer;

public class StringBuilder01 {
    public static void main(String[] args) {

        String s = "Java";
        String t = s + "!"; // original s doesn't change. but 2 memory spaces for s and t are created.

        StringBuilder s1 = new StringBuilder("Java");
        System.out.println(s1);
        s1.append("!");
        System.out.println(s1); //original s gone, less memory uses
        s1.insert(4," is love");
        System.out.println(s1);

        s1.insert(1,"brook",1,4);
        System.out.println(s1);

        s1.replace(3,5,"frokun");
        System.out.println(s1);

        StringBuilder s2 = new StringBuilder("abcdefg");
        s2.reverse();
        System.out.println(s2);

        StringBuilder s3 = new StringBuilder("Auvam");
        StringBuilder s4 = new StringBuilder("aavak");

        int r = s3.compareTo(s4);  // compare alphabetical order difference by Ascii numbers
        System.out.println(r);


        String m = "Java";
        System.out.println(m);
        m = m.concat("k");
        System.out.println(m);

    }
}