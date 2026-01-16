package challenges.utilities;


import challenges.stacksQueues.Stack;
import challenges.stacksQueues.LinkedListStack;

import java.util.Map;
import static java.util.Map.entry;

public class MultiBracketValidation {
    static Map<Character, Character> pairs = Map.ofEntries(
            entry('{', '}'),
            entry('[', ']'),
            entry('(', ')')
    );

    public static boolean multiBracketValidation(String input) {
        Stack<Character> stack = new LinkedListStack<>();
        for(char c : input.toCharArray()) {
            if (pairs.containsKey(c)) stack.push(c);    // When c is an open bracket
            if (pairs.containsValue(c)) {               // When c is a closing bracket
                if (stack.isEmpty() || pairs.get(stack.peek()) != c) return false;
                stack.pop();
            }
        }
        return stack.isEmpty();
    }
}