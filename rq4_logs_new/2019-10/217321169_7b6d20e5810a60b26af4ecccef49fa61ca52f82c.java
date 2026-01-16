package cn.zynworld.leetcode.Q300;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

/**
 * @author zhaoyuening
 */
public class Q241 {

    public static Integer getResult(String calString) {
        // 转为后缀表达式
        char[] chars = convert(calString).toCharArray();

        Stack<Integer> numberStack = new Stack<>();
        for (char c : chars) {
            // 如果是数字
            if ((c - '0' <= 9 && c - '0' >= 0)) {
                numberStack.add(c - '0');
                continue;
            }

            // 如果是符号
            Integer b = numberStack.pop();
            Integer a = numberStack.pop();
            switch (c) {
                case '+':
                    numberStack.add(a + b);
                    break;
                case '-':
                    numberStack.add(a - b);
                    break;
                case '*':
                    numberStack.add(a * b);
                    break;
                case '/':
                    numberStack.add(a / b);
                    break;
            }
        }

        return numberStack.peek();
    }

    /**
     * 将中缀表达式转化为后缀表达式
     * @param calString
     * @return
     */
    private static String convert(String calString) {
        StringBuilder builder = new StringBuilder();
        char[] chars = calString.toCharArray();
        Stack<Character> stack = new Stack<>();
        for (char c : chars) {
            // 如果是数字
            if ((c - '0' <= 9 && c - '0' >= 0)) {
                builder.append(c);
                continue;
            }

            // 如果是 (
            if (c == '(') {
                stack.add(c);
                continue;
            }

            // 如果是 )
            if (c == ')') {
                while (!stack.isEmpty() && stack.peek() != '(') {
                    builder.append(stack.pop());
                }
                if (!stack.isEmpty() && stack.peek() == '(') {
                    stack.pop();
                }
                continue;
            }

            // 如果是其他类型符号
            Integer charLevel = charLevel(c);
            // 将优先级高于当前字符的弹出
            while (!stack.isEmpty() && charLevel <= charLevel(stack.peek())) {
                builder.append(stack.pop());
            }
            stack.add(c);
        }

        while (!stack.isEmpty()) {
            builder.append(stack.pop());
        }

        return builder.toString();
    }

    private static Integer charLevel(char c) {
        switch (c) {
            case '*':
            case '/':
                return 3;
            case '+':
            case '-':
                return 2;
            default:
                return 0;
        }
    }

    public static void main(String[] args) {
        String calString = "2*(1+2)";
        System.out.println(getResult(calString));
    }

}