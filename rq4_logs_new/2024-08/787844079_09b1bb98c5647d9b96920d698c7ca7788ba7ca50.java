//https://leetcode.com/problems/detect-capital/description/
package algorithms.easy;

public class DetectCapital {
    public static void main(String[] args) {
        DetectCapital capital = new DetectCapital();
        System.out.println("Output:\t" + capital.detectCapitalUse("USA"));
        System.out.println("Output:\t" + capital.detectCapitalUseOptimized("FlaG"));
    }

    public boolean detectCapitalUse(String word) {
        int n = word.length();
        int[] ascii = new int[n];
        for (int i = 0; i < n; i++)
            ascii[i] = word.charAt(i) - 'A';

        int capitals = 0;
        for (int num : ascii)
            if (num < 26) capitals++;

        if (capitals == n || capitals == 0) return true;
        if (capitals == 1 && ascii[0] < 26) return true;

        return false;
    }

    public boolean detectCapitalUseOptimized(String word) {
        int capitals = 0;

        for (char ch : word.toCharArray())
            if (Character.isUpperCase(ch)) capitals++;

        return capitals == 0 || capitals == word.length() || (capitals == 1 && Character.isUpperCase(word.charAt(0)));

    }
}