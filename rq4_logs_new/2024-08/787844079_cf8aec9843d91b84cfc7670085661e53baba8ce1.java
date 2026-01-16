//https://leetcode.com/problems/repeated-substring-pattern/
package algorithms.easy;

public class RepeatedSubstringPattern {
    public static void main(String[] args) {
        RepeatedSubstringPattern repeatedSub = new RepeatedSubstringPattern();
        System.out.println("Output:\t" + repeatedSub.repeatedSubstringPattern("abab"));
        System.out.println("Output:\t" + repeatedSub.repeatedSubstringPatternOptimized("aba"));
        System.out.println("Output:\t" + repeatedSub.repeatedSubstringPatternOptimized("abcabcabcabc"));
    }

    public boolean repeatedSubstringPatternOptimized(String s) {
        String doubledString = s + s;
        return doubledString.substring(1, doubledString.length() - 1).contains(s);
    }

    public boolean repeatedSubstringPattern(String s) {
        int n = s.length();
        for (int i = 1; i <= n / 2; i++) {
            if (n % i == 0) {
                String sub = s.substring(0, i);
                StringBuilder repeat = new StringBuilder();
                for (int j = 0; j < n / i; j++) {
                    repeat.append(sub);
                }
                if (repeat.toString().equals(s))
                    return true;
            }
        }
        return false;
    }
}