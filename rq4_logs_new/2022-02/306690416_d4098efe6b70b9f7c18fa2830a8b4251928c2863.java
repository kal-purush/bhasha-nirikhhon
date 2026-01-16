package array.CompareVersionNumbers165;

public class Solution {

    public static void main(String[] str) {
        Solution sol = new Solution();
        sol.compareVersion("0.1", "1.1");
    }

    public int compareVersion(String version1, String version2) {
        String[] v1Str = version1.split("\\.");
        int len1 = v1Str.length;

        String[] v2Str = version2.split(".");
        int len2 = v2Str.length;

        int index = 0;

        int v1Cur, v2Cur;

        while (index < len1 || index < len2) {
            v1Cur = index < len1? Integer.parseInt(v1Str[index]): 0;
            v2Cur = index < len2? Integer.parseInt(v2Str[index]): 0;

            if (v1Cur > v2Cur) {
                return 1;
            } else if (v2Cur > v1Cur) {
                return -1;
            }

            index++;
        }

        return 0;
    }
}