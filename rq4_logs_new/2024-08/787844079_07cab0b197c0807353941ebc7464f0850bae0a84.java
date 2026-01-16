//https://leetcode.com/problems/reverse-string-ii/
package algorithms.easy;

public class ReverseStringII {
    public static void main(String[] args) {
        ReverseStringII reverse = new ReverseStringII();
        System.out.println("Output:\t" + reverse.reverseStr("abcdefg", 2));
        System.out.println("Output:\t" + reverse.reverseStr("abcd", 2));
    }

    public String reverseStr(String s, int k) {
        char[] arr = s.toCharArray();
        for (int i = 0; i < s.length(); i += 2 * k) {
            int start = i;
            int end = Math.min(s.length() - 1, i + k - 1);
            while (start < end) {
                char temp = arr[start];
                arr[start] = arr[end];
                arr[end] = temp;
                start++;
                end--;
            }
        }
        return new String(arr);
    }
}