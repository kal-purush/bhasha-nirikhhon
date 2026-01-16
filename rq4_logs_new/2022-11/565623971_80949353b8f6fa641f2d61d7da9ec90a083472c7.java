package main.java.com.yearup.week4.code.stack;

//https://www.geeksforgeeks.org/check-for-balanced-parentheses-in-an-expression/
//Time Complexity: O(N), Iteration over the string of size N one time.
//Auxiliary Space: O(N) for stack.

/*
Follow the steps mentioned below to implement the idea:

Declare a character stack (say temp).
Now traverse the string exp.
If the current character is a starting bracket ( ‘(‘ or ‘{‘  or ‘[‘ ) then push it to stack.
If the current character is a closing bracket ( ‘)’ or ‘}’ or ‘]’ ) then pop from stack and if the popped character is the matching starting bracket then fine.
Else brackets are Not Balanced.
After complete traversal, if there is some starting bracket left in stack then Not balanced, else Balanced.
 */

import java.util.ArrayDeque;
import java.util.Deque;

public class BalanceCheckParamUsingStack {

    // function to check if brackets are balanced
    static boolean areBracketsBalanced(String expr) {
        // Using ArrayDeque is faster than using Stack class
        Deque<Character> stack
                = new ArrayDeque<Character>();

        // Traversing the Expression
        for (int i = 0; i < expr.length(); i++) {
            char x = expr.charAt(i);

            if (x == '(' || x == '[' || x == '{') {
                // Push the element in the stack
                stack.push(x);
                continue;
            }

            // If current character is not opening
            // bracket, then it must be closing. So stack
            // cannot be empty at this point.
            if (stack.isEmpty())
                return false;
            char check;
            switch (x) {
                case ')':
                    check = stack.pop();
                    if (check == '{' || check == '[')
                        return false;
                    break;

                case '}':
                    check = stack.pop();
                    if (check == '(' || check == '[')
                        return false;
                    break;

                case ']':
                    check = stack.pop();
                    if (check == '(' || check == '{')
                        return false;
                    break;
            }
        }

        // Check Empty Stack
        return (stack.isEmpty());
    }

    public static void main(String[] args) {
        String expr = "([{}])";

        // Function call
        if (BalanceCheckParamUsingStack.areBracketsBalanced(expr))
            System.out.println("Balanced ");
        else
            System.out.println("Not Balanced ");
    }
}