/**
 * 罗马数字转为整数
 */

class Solution {
    public int romanToInt(String s) {
        int result = 0;

        // I:1, V:5, X:10, L:50, C:100, D:500, M:1000
        char[] romans = s.toCharArray();
        for (int i = 0; i < romans.length - 1; ++i) {
            if (romans[i] == 'I' && (romans[i + 1] == 'V' || romans[i + 1] == 'X')) {
                result -= romanVal(romans[i]);
            }
            else if (romans[i] == 'X' && (romans[i + 1] == 'L' || romans[i + 1] == 'C')) {
                result -= romanVal(romans[i]);
            }
            else if (romans[i] == 'C' && (romans[i + 1] == 'D' || romans[i + 1] == 'M')) {
                result -= romanVal(romans[i]);
            }
            else {
                result += romanVal(romans[i]);
            }
        }

        result += romanVal(romans[romans.length - 1]);

        return result;
    }

    public int romanVal(char c) {
        int val = 0;
        switch (c) {
            case 'I' :
                val = 1;
                break;
            case 'V' :
                val = 5;
                break;
            case 'X' :
                val = 10;
                break;
            case 'L' :
                val = 50;
                break;
            case 'C' :
                val = 100;
                break;
            case 'D' :
                val = 500;
                break;
            case 'M' :
                val = 1000;
                break;
        }

        return val;
    }
}

class Test {
    public static void main(String[] args) {
        Solution s = new Solution();

        System.out.println(s.romanToInt("MCMXCIV"));
    }
}