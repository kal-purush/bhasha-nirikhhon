package cn.tiny77.algorithm.from0000to0100.n038;

class Solution {

    public static void main(String[] args) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(1).append("1");
        System.out.printf(stringBuilder.toString());
    }

    public String countAndSay(int n) {
        if (n == 1) {
            return "1";
        } else {
            String result = countAndSay(n - 1);
            char[] m = new char[result.length()];
            int[] num = new int[result.length()];
            int top = 0;
            for (char c : result.toCharArray()) {
                if (top == 0 || m[top - 1] != c) {
                    m[top] = c;
                    num[top] = 1;
                    top ++;
                } else {
                    num[top - 1] ++;
                }
            }
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < top; i ++) {
                stringBuilder.append(num[i]).append(m[i]);
            }
            return stringBuilder.toString();
        }
    }
}