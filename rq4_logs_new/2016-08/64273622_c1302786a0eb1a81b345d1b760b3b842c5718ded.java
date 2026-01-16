package ch1;

import util.Question;

public class IsRotation extends Question {

    public IsRotation() {
        super("1.8", "Given an 'isSubstring' method, check if string X is a rotation of string Y by calling 'isSubstring' just once");
    }

    @Override
    public void run() {
        hello();
        String a = "chestnut";
        String a_rot = "nutchest";
        printRes(a, a_rot);

        String b = "arora";
        String b_rot = "roraa";
        printRes(b, b_rot);

        String c = "bananaman";
        String c_rot = "namanbana";
        printRes(c, c_rot);

        String d = "a";
        String d_rot = "a";
        printRes(d, d_rot);

        String e = "a";
        String e_rot = "b";
        printRes(e, e_rot);

        String f = "ab";
        String f_rot = "aa";
        printRes(f, f_rot);

        String g = "abc";
        String g_rot = "cabd";
        printRes(g, g_rot);

        bye();
    }

    private void printRes(String first, String second) {
        Boolean isRot = isRotation(first, second);
        System.out.printf("%10s %10s - %1b\n", first, second, isRot);
    }

    /**
     *
     * @param s1
     * @param s2
     * @return true if s2 is rotation of s1
     */
    public boolean isRotation(String s1, String s2) {
        if (s1.length() == 0 || s1.length() != s2.length()) {
            return false;
        }

        String s2s2 = s2 + s2;
        return isSubstring(s1, s2s2);

    }

    public Boolean isSubstring(String first, String second) {
        return second.indexOf(first) >= 0;
    }
}