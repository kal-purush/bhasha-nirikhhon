import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 括号生成
 * 递归
 */
class Solution {
    public List<String> generateParenthesis(int n) {
        ArrayList<String> result = new ArrayList<>();

        if (n <= 0) {
            return null;
        }
        else if (n == 1) {
            result.add("()");
            return result;
        }
        else if (n == 2) {
            result.add("(())");
            result.add("()()");
            return result;
        }
        else {
            // n >= 3
            // Eliminate duplicate combinations
            Set<String> set = new HashSet<>();
            for (String s : generateParenthesis(n - 1)) {
                set.add("(" + s + ")");
                set.add(s + "()");
                set.add("()" + s);

            }

            for (int i = 2; i < n - 1; ++i) {
                for (String s : generateParenthesis(n - i)) {
                    for (String t : generateParenthesis(i)) {
                        set.add(s + t);
                    }
                }
            }

            for (String s : set) {
                result.add(s);
            }

            return result;
        }

    }
}

class Test {
    public static void main(String[] args) {
        Solution s = new Solution();
        System.out.println(s.generateParenthesis(4));
    }
}