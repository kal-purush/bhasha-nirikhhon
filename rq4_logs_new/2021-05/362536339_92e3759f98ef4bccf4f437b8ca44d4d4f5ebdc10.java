import java.util.Stack;

/*
Given a string s containing just the characters '(', ')', '{', '}', '[' and ']', determine if the input string is valid.

An input string is valid if:

    Open brackets must be closed by the same type of brackets.
    Open brackets must be closed in the correct order.
    Example 1:

    Input: s = "()"
    Output: true

    Example 2:

    Input: s = "()[]{}"
    Output: true

    Example 3:

    Input: s = "(]"
    Output: false
*/

public class ValidParenthesis{

    public boolean isValid(String s) {
        // s is a String; So convert it to char array
        char[] c = s.toCharArray();
        Stack stack = new Stack<Character>();

        if (c.length % 2 != 0){
            return false;
        }

        // Other type of for-each loop
        // for (Character ch : c) {
        // }

        for(int i = 0; i < c.length; i++){
            if(c[i] == '{' || c[i] == '(' || c[i] == '['){
                stack.push(c[i]);
            }

            if(c[i] == '}' && stack.peek().equals('{')){
                stack.pop();
            }
            if(c[i] == ')' && stack.peek().equals('(')){
                stack.pop();
            }
            if(c[i] == ']' && stack.peek().equals('[')){
                stack.pop();
            }
        }

        if(stack.empty() == true){
            return true;
        }
        return false;
    }

    public static void main(String[] args) {
        ValidParenthesis a = new ValidParenthesis();
        System.out.println("([]) : " + a.isValid("([])") );
        System.out.println("()[]{} : " + a.isValid("()[]{}") );
        System.out.println("{()[]{} : " + a.isValid("{()[]{}") );
        System.out.println("{)[]{} : " + a.isValid("{)[]{}") );

    }

}