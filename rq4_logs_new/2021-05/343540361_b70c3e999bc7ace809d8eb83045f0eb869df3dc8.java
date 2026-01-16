package problem150;

import java.util.Deque;
import java.util.LinkedList;

public final class ReversePolishNotation {

    public int evalRPN(final String[] tokens) {
        final Deque<Integer> rpnStack = new LinkedList<>();
        int op1, op2;

        for (var t : tokens) {
            switch (t) {
                case "+":
                    rpnStack.offer(rpnStack.removeLast() + rpnStack.removeLast());
                    break;
                case "-":
                    op2 = rpnStack.removeLast();
                    op1 = rpnStack.removeLast();
                    rpnStack.offer(op1 - op2);
                    break;
                case "*":
                    rpnStack.offer(rpnStack.removeLast() * rpnStack.removeLast());
                    break;
                case "/":
                    op2 = rpnStack.removeLast();
                    op1 = rpnStack.removeLast();
                    rpnStack.offer(op1 / op2);
                    break;
                default:
                    rpnStack.add(Integer.parseInt(t));
            }
        }

        return rpnStack.getLast();
    }

}