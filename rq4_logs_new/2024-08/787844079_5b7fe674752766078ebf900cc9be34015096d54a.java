//https://leetcode.com/problems/reverse-words-in-a-string-iii/description/
package algorithms.easy;

public class ReverseWordsInAStringIII {
    public static void main(String[] args) {
        ReverseWordsInAStringIII reverse = new ReverseWordsInAStringIII();
        System.out.println("Output:\t" + reverse.reverseWords("Let's take LeetCode contest"));
        System.out.println("Output:\t" + reverse.reverseWords("Mr Ding"));
    }

    public String reverseWords(String s) {
        StringBuilder sb = new StringBuilder();
        for (String str : s.split(" ")) {
            StringBuilder sbi = new StringBuilder();
            sbi.append(str);
            sb.append(sbi.reverse()).append(" ");
        }
        return sb.toString().trim();
    }
}