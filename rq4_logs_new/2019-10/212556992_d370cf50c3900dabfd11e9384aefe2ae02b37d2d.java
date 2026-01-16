/**
 * 寻找最长回文子串
 * 动态规划
 */
class Solution {
    public String longestPalindrome(String s) {
        if (s.length() == 0 || s == null) {
            return "";
        }

        int len = s.length();
        char[] chars = s.toCharArray();
        boolean[][] matrix = new boolean[len][len];
        for (int i = 0; i < len; ++i) {
            for (int j = 0; j < len; ++j) {
                matrix[i][j] = false;
            }
        }

        int indexHead = 0;
        int indexTail = 0;

        // 1
        for (int i = 0; i < s.length(); ++i) {
            matrix[i][i] = true;
        }

        // 2
        for (int i = 0; i < len - 1; ++i) {
            if (chars[i] == chars[i + 1]) {
                if (indexTail - indexHead == 0) {
                    indexHead = i;
                    indexTail = i + 1;
                }
                matrix[i][i + 1] = true;
            }
        }

        for (int i = 3; i < len + 1; ++i) {
            for (int j = 0; j < len - i + 1; ++j) {
                if (matrix[j + 1][j + i - 2] && chars[j] == chars[j + i - 1]) {
                    matrix[j][j + i - 1] = true;

                    if (indexTail - indexHead < i - 1) {
                        indexHead = j;
                        indexTail = j + i - 1;
                    }
                }
            }
        }

//        System.out.println("indexHead = " + indexHead + ", indexTail = " + indexTail);
        return s.substring(indexHead, indexTail + 1);
    }
}

class Test {
    public static void main(String[] args) {
        Solution s = new Solution();
        System.out.println(s.longestPalindrome("cccc"));
    }
}
