package org.example.algo;

import java.util.Iterator;
import java.util.Stack;

public class RemoveAllaAdjacentDuplicates {

    public String removeDuplicates(String s) {
        Stack<Character> stack = new Stack<Character>();
        for(Character c : s.toCharArray()) {
            if(!stack.isEmpty() && stack.peek() == c) {
                stack.pop();
            }
            else {
                stack.push(c);
            }
        }
        StringBuilder sb = new StringBuilder();
        /*Iterator<Character> iterator = stack.iterator();
        while(iterator.hasNext()) {
            sb.append(iterator.next());
        }
        return sb.toString();
        // Alternatively, with just stack standard calls is the following code:
        */
        while (!stack.empty()) {
            sb.append(stack.pop());
        }
        return sb.reverse().toString();
    }


    public static void main(String[] args) {
        RemoveAllaAdjacentDuplicates raad = new RemoveAllaAdjacentDuplicates();
        System.out.println(raad.removeDuplicates("abbacad"));
    }
}

