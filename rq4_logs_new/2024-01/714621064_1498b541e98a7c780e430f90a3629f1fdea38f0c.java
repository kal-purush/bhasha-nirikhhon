package LeetCode_Daily;

import java.util.Stack;

public class Evaluate_Reverse_Polish_Notation {
    public static void main(String[] args) {
        String[] arr = {"10","6","9","3","+","-11","*","/","*","17","+","5","+"};
        System.out.println(evalRPN(arr));
    }

    public static int evalRPN(String[] tokens) {
        Stack<Integer> stack = new Stack<>();

        for (int i = 0; i < tokens.length; i++) {
            if (tokens[i].equals("+")) {
                int frontNum = stack.pop();
                int backNum = stack.pop();
                int sum = backNum + frontNum;
                stack.push(sum);

            } else if (tokens[i].equals("-")) {
                int frontNum = stack.pop();
                int backNum = stack.pop();
                int sum = backNum - frontNum;
                stack.push(sum);

            } else if (tokens[i].equals("*")) {
                int frontNum = stack.pop();
                int backNum = stack.pop();
                int sum = backNum * frontNum;
                stack.push(sum);

            } else if (tokens[i].equals("/")) {
                int frontNum = stack.pop();
                int backNum = stack.pop();
                int sum = backNum / frontNum;
                stack.push(sum);

            } else {
                stack.push(Integer.parseInt(tokens[i]));
            }
        }


        return stack.peek();
    }

}