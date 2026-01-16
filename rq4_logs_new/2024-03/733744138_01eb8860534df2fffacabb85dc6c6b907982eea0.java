package br.dev.ferreiras.challenges.leetCode;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Stack;

/*
Given two strings s and t, return true if they are equal when both are typed into empty text editors.
'#' means a backspace character.

Note that after backspacing an empty text, the text will continue empty.

Example 1:

Input: s = "ab#c", t = "ad#c"
Output: true
Explanation: Both s and t become "ac".
Example 2:

Input: s = "ab##", t = "c#d#"
Output: true
Explanation: Both s and t become "".
Example 3:

Input: s = "a#c", t = "b"
Output: false
Explanation: s becomes "c" while t becomes "b".


Constraints:

1 <= s.length, t.length <= 200
s and t only contain lowercase letters and '#' characters.

 */
public class BackspaceStringCompare {

  public static void main(String[] args) {
    String s = "ab##", t = "c#d#";
    boolean f = backspaceCompare(s, t);
    System.out.println(f);
  }

  public static boolean backspaceCompare(String s, String t) {

    Stack<Character> st1 = new Stack<>();
    Stack<Character> st2 = new Stack<>();

    for (char c : s.toCharArray()) {

      if (c == '#') {
        if (!st1.isEmpty()) {
          st1.pop();
        }
      } else {
        st1.push(c);
      }

    }

    for (char c : t.toCharArray()) {

      if (c == '#') {
        if (!st2.isEmpty()) {
          st2.pop();
        }
      } else {
        st2.push(c);
      }

    }

    return st1.equals(st2);
  }
}