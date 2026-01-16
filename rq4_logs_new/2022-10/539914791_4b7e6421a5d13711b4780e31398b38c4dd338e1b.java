package org.example.algo;
import java.util.*;

public class GoodString {

    public String makeGood(String s) {
        Stack<Character> stack = new Stack<Character>();

        for(Character c : s.toCharArray()) {
            if (!stack.isEmpty() && Math.abs(stack.peek() - c) == 32) {
                stack.pop();
            }
            else
                stack.push(Character.valueOf(c));
        }

        StringBuilder sb = new StringBuilder();
        for(Character c: stack) {
            sb.append(c);
        }
        return sb.toString();
    }


    public char[] copyArrayExceptTwoIndex(char[] array, int exceptI, int exceptY) {
        List<Character> copyList = new ArrayList<>();
        for(int i =0; i< array.length; i++) {
            if(i==exceptI || i==exceptY) continue;
            else copyList.add(array[i]);
        }
        final char[] copyArray = new char[copyList.size()];
        for(int i = 0; i < copyArray.length; i++)
            array[i] = copyList.get(i);
        return copyArray;
    }

    public static void main(String[] args) {
        GoodString gs = new GoodString();
        System.out.println(gs.makeGood("leEeetcode"));

    }
}