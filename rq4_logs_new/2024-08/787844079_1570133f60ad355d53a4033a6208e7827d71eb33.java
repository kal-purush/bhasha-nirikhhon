//https://leetcode.com/problems/longest-uncommon-subsequence-i/
package algorithms.easy;

public class LongestUncommonSubsequenceI {
    public static void main(String[] args) {
        LongestUncommonSubsequenceI longest = new LongestUncommonSubsequenceI();
        System.out.println("Output:\t" + longest.findLUSlength("aba", "cdc"));
        System.out.println("Output:\t" + longest.findLUSlength("aaa", "bbb"));
        System.out.println("Output:\t" + longest.findLUSlength("aaa", "aaa"));
    }

    public int findLUSlength(String a, String b) {
        if (a.equals(b)) return -1;
        else return Math.max(a.length(), b.length());
    }
}