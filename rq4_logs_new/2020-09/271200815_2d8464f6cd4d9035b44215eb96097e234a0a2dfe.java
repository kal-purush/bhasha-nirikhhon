package com.practice.random;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Stack;

public class EvaluatingPostFix {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String[] s = br.readLine().split(" ");
        ArrayList<String> list = new ArrayList<>();
        for (int i = 0; i < s.length; i++)
            list.add(i, s[i]);
        int ans = evaluateExpression(list);
        System.out.println(ans);
    }

    private static int evaluateExpression(ArrayList<String> list) {

        Stack<Integer> stack = new Stack<>();
        for (String token : list){
            if (token.equals("+") || token.equals("-") || token.equals("*") || token.equals("/")) {
                int operand2 = stack.pop();
                int operand1 = stack.pop();
                int result = doMath(token, operand1, operand2);
                stack.push(result);
            }
            else {
                stack.push(Integer.parseInt(token));
            }
        }
        return stack.peek();
    }

    private static int doMath(String token, int operand1, int operand2) {
        if (token.equals("*"))
            return operand1 * operand2;
        else if (token.equals("/"))
            return operand1 / operand2;
        else if (token.equals("+"))
            return operand1 + operand2;
        else
            return operand1 - operand2;
    }
}