//https://leetcode.com/problems/generate-parentheses/description/
package algorithms.easy;

import java.util.ArrayList;
import java.util.List;

public class GenerateParentheses {
    List<String> answer = new ArrayList<>();
    int max;

    public static void main(String[] args) {
        GenerateParentheses generate = new GenerateParentheses();
        System.out.println("Output:\t" + generate.generateParenthesis(3));
        System.out.println("Output:\t" + generate.generateParenthesis(1));
    }

    public List<String> generateParenthesis(int n) {
        this.max = n;
        backtrack("", 0, 0);
        return answer;
    }

    public void backtrack(String current, int open, int close) {
        if (current.length() == max * 2) {
            answer.add(current);
            return;
        }

        if (open < max) backtrack(current + "(", open + 1, close);
        if (close < open) backtrack(current + ")", open, close + 1);
    }
}