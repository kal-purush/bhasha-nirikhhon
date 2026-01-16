package InterviewCoding;

import java.util.ArrayList;

public class RepeatedChars {

    public static ArrayList<Character> myString(String a) {

        ArrayList<Character> d = new ArrayList<>();
        char[] c = a.toCharArray();
        for (int i = 0; i < c.length; i++) {
            boolean isUnique = true;
            for (int j = 0; j < c.length; j++) {
                if (i != j && c[i] == c[j]) {
                    isUnique = false;
                    break;
                }
            }
            if (isUnique) {
                d.add(c[i]);
            }

        }
        System.out.println(d);

        return d;
    }

    public static void main(String[] args) {
        myString("Enver is a good programmer");
    }
}