package StacksQueuesProjects;

import java.util.*;

//This project rearanges stack to queue or queue to stack for different purposes
public class Project1 {
    public static void main(String[] args) {
        System.out.println("Exercise 1");
        Stack<Integer> ints = new Stack<Integer>();
        ints.push(3);
        ints.push(-5);
        ints.push(1);
        ints.push(2);
        ints.push(-4);
        System.out.println("Before: " + ints);
        splitStack(ints);
        System.out.println("After: " + ints);
        System.out.println();

        System.out.println("Exercise 2");
        Stack<Integer> nums = new Stack<Integer>();
        nums.push(3);
        nums.push(7);
        nums.push(1);
        nums.push(14);
        nums.push(9);
        System.out.println("Before: " + nums);
        stutter(nums);
        System.out.println("After: " + nums);
        System.out.println();

        System.out.println("Exercise 3");
        Stack<Integer> stack = new Stack<Integer>();
        stack.push(1);
        stack.push(2);
        stack.push(3);
        System.out.println("Before: " + stack);
        Stack<Integer> copy = copyStack(stack);
        System.out.println("After: " + copy);
        System.out.println();

        System.out.println("Exercise 4");
        Stack<Integer> s = new Stack<Integer>();
        s.push(7);
        s.push(2);
        s.push(8);
        s.push(9);
        s.push(4);
        s.push(11);
        s.push(7);
        s.push(1);
        s.push(42);
        System.out.println("Before: " + s);
        collapse(s);
        System.out.println("After: " + s);
        System.out.println();

        System.out.println("Exercise 5");
        Stack<Integer> s1 = new Stack<Integer>();
        s1.push(1);
        s1.push(2);
        s1.push(3);
        Stack<Integer> s2 = new Stack<Integer>();
        s2.push(1);
        s2.push(2);
        s2.push(3);
        System.out.println(s1);
        System.out.println(s2);
        boolean same = equals(s1, s2);
        System.out.println(same);
        System.out.println();

        System.out.println("Exercise 6");
        Queue<Integer> q = new LinkedList<Integer>();
        q.add(3);
        q.add(5);
        q.add(4);
        q.add(17);
        q.add(6);
        q.add(83);
        q.add(1);
        q.add(84);
        q.add(16);
        q.add(37);
        System.out.println("Before: " + q);
        rearrange(q);
        System.out.println("After: " + q);
        System.out.println();

        System.out.println("Exercise 7");
        Queue<Integer> evodd = new LinkedList<Integer>();
        evodd.add(1);
        evodd.add(8);
        evodd.add(7);
        evodd.add(2);
        evodd.add(9);
        evodd.add(18);
        evodd.add(12);
        evodd.add(0);
        evodd.add(21);
        System.out.println("Before: " + evodd);
        reverseHalf(evodd);
        System.out.println("After: " + evodd);
        System.out.println();

        System.out.println("Exercise 8");
        Queue<Integer> queue = new LinkedList<Integer>();
        queue.add(3);
        queue.add(8);
        queue.add(17);
        queue.add(9);
        queue.add(17);
        queue.add(8);
        queue.add(3);
        boolean equal = isPalindrome(queue);
        System.out.println(equal);
        System.out.println();

        System.out.println("Exercise 9");
        Stack<Integer> st = new Stack<Integer>();
        st.push(1);
        st.push(2);
        st.push(8);
        st.push(6);
        st.push(-1);
        st.push(15);
        st.push(7);
        System.out.println("Before: " + st);
        switchPairs(st);
        System.out.println("After: " + st);
        System.out.println();

        System.out.println("Exercise 10");
        Stack<Integer> consec = new Stack<Integer>();
        consec.push(5);
        consec.push(6);
        consec.push(7);
        consec.push(8);
        consec.push(9);
        consec.push(10);
        boolean yes = isConsecutive(consec);
        System.out.println(yes);
        System.out.println();

        System.out.println("Exercise 11");
        Queue<Integer> values = new LinkedList<Integer>();
        values.add(1);
        values.add(2);
        values.add(-2);
        values.add(4);
        values.add(-5);
        values.add(8);
        values.add(-8);
        values.add(12);
        values.add(-15);
        System.out.println("Before: " + values);
        reorder(values);
        System.out.println("After: " + values);
        System.out.println();

        System.out.println("Exercise 12");
        Stack<Integer> stuff = new Stack<Integer>();
        stuff.push(1);
        stuff.push(2);
        stuff.push(3);
        stuff.push(4);
        stuff.push(5);
        stuff.push(6);
        stuff.push(7);
        stuff.push(8);
        System.out.println("Before: " + stuff);
        shift(stuff, 3);
        System.out.println("After: " + stuff);
        System.out.println();

        System.out.println("Exercise 13");
        Stack<Integer> things = new Stack<Integer>();
        things.push(4);
        things.push(20);
        things.push(15);
        things.push(15);
        things.push(8);
        things.push(5);
        things.push(7);
        things.push(12);
        things.push(3);
        things.push(10);
        things.push(5);
        things.push(1);
        System.out.println("Before: " + things);
        expunge(things);
        System.out.println("After: " + things);
        System.out.println();

        System.out.println("Exercise 14");
        Queue<Integer> nice = new LinkedList<Integer>();
        nice.add(10);
        nice.add(20);
        nice.add(30);
        nice.add(40);
        nice.add(50);
        nice.add(60);
        nice.add(70);
        nice.add(80);
        nice.add(90);
        System.out.println("Before: " + nice);
        reverseFirstK(4, nice);
        System.out.println("After: " + nice);
        System.out.println();

        System.out.println("Exercise 15");
        Stack<Integer> sorted = new Stack<Integer>();
        sorted.push(20);
        sorted.push(20);
        sorted.push(17);
        sorted.push(11);
        sorted.push(8);
        sorted.push(9);
        sorted.push(3);
        sorted.push(2);
        boolean check = isSorted(sorted);
        System.out.println(check);
        System.out.println();

        System.out.println("Exercise 16");
        System.out.println("Before: " + ints);
        mirror(ints);
        System.out.println("After: " + ints);
        System.out.println();

        System.out.println("Exercise 17");
        Stack<Integer> dup = new Stack<Integer>();
        dup.push(2);
        dup.push(2);
        dup.push(2);
        dup.push(2);
        dup.push(2);
        dup.push(-4);
        dup.push(-4);
        dup.push(-4);
        dup.push(82);
        dup.push(6);
        dup.push(6);
        dup.push(6);
        dup.push(6);
        dup.push(17);
        dup.push(17);
        System.out.println("Before: " + dup);
        compressDuplicates(dup);
        System.out.println("After: " + dup);
        System.out.println();

        System.out.println("Exercise 18");
        Queue<Integer> digits = new LinkedList<Integer>();
        digits.add(10);
        digits.add(50);
        digits.add(19);
        digits.add(54);
        digits.add(30);
        digits.add(67);
        System.out.println("Before: " + digits);
        mirrorHalves(digits);
        System.out.println("After: " + digits);
        System.out.println();

        System.out.println("Exercise 19");
        System.out.println("Before: " + ints);
        int min = removeMin(ints);
        System.out.println("Min: " + min);
        System.out.println("After: " + ints);
        System.out.println();

        System.out.println("Exercise 20");
        System.out.println("Before: " + digits);
        interleave(digits);
        System.out.println("After: " + digits);
    }

    // This method changes a queue to a stack
    public static void queueToStack(Queue<Integer> q, Stack<Integer> s) {
        while (!q.isEmpty())
            s.push(q.remove());
    }

    // This method changes a stack to a queue
    public static void stackToQueue(Stack<Integer> s, Queue<Integer> q) {
        while (!s.isEmpty())
            q.add(s.pop());
    }

    // splitStack accepts a stack of integers as a parameter and rearranges its
    // elements
    // so that all the negatives appear on the bottom of the stack and all the
    // nonnegatives appear on the top.
    public static void splitStack(Stack<Integer> s) {
        Queue<Integer> q = new LinkedList<Integer>();
        stackToQueue(s, q);
        int size = q.size();
        for (int i = 0; i < size; i++) {
            int n = q.remove();
            if (n < 0)
                q.add(n);
            else
                s.push(n);
        }

        while (!q.isEmpty())
            s.push(q.remove());
        stackToQueue(s, q);
        queueToStack(q, s);
    }

    // stutter accepts a stack of integers as a parameter and replaces every value
    // in the stack with two occurrences of that value.
    // Preserve the original relative order.
    public static void stutter(Stack<Integer> s) {
        Queue<Integer> q = new LinkedList<Integer>();
        while (!s.isEmpty()) {
            int n = s.pop();
            for (int i = 0; i < 2; i++)
                q.add(n);
        }
        queueToStack(q, s);
        stackToQueue(s, q);
        queueToStack(q, s);
    }

    // copyStack accepts a stack of integers as a parameter and returns a copy of
    // the original stack
    // (i.e., a new stack with the same values as the original, stored in the same
    // order as the original).
    public static Stack<Integer> copyStack(Stack<Integer> s) {
        Stack<Integer> newStack = new Stack<Integer>();
        Queue<Integer> q = new LinkedList<Integer>();
        while (!s.isEmpty())
            newStack.push(s.pop());
        stackToQueue(newStack, q);
        queueToStack(q, newStack);
        queueToStack(q, s);
        return newStack;
    }

    // collapse accepts a stack of integers as a parameter and that collapses it by
    // replacing each successive pair of integers with the sum of the pair.
    public static void collapse(Stack<Integer> s) {
        Queue<Integer> q = new LinkedList<Integer>();
        int sum = 0;
        int size = s.size();
        while (!s.isEmpty())
            q.add(s.pop());
        if (size % 2 != 0) {
            s.push(q.remove());
            size = (size - 1) / 2;
        }
        for (int i = 1; i <= size; i++) {
            sum += q.remove();
            sum += q.remove();
            s.push(sum);
            sum = 0;
        }
        stackToQueue(s, q);
        queueToStack(q, s);
    }

    // equals accepts two stacks of integers as parameters and
    // returns true if the two stacks store exactly the same sequence of integer
    // values in the same order.
    public static boolean equals(Stack<Integer> s1, Stack<Integer> s2) {
        Stack<Integer> s3 = new Stack<Integer>();
        if (s1.size() != s2.size())
            return false;
        while (!s1.isEmpty()) {
            int n = s2.pop();
            s3.push(n);
            if (s1.pop() != n)
                return false;
        }
        while (!s3.isEmpty()) {
            int n = s3.pop();
            s1.push(n);
            s2.push(n);
        }
        return true;
    }

    // rearrange accepts a queue of integers as a parameter and rearranges the order
    // of the values
    // so that all of the even values appear before the odd values and that
    // otherwise preserves the original order of the queue.
    public static void rearrange(Queue<Integer> q) {
        Stack<Integer> s = new Stack<Integer>();
        int size = q.size();
        for (int i = 1; i <= 2; i++) {
            for (int j = 0; j < size; j++) {
                int n = q.remove();
                if (n % 2 != 0)
                    s.push(n);
                else
                    q.add(n);
            }
            while (!s.isEmpty())
                q.add(s.pop());
        }
    }

    // reverseHalf accepts a queue of integers as a parameter and reverses the order
    // of all the elements in odd-numbered positions
    // (position 1, 3, 5, etc.), assuming that the first value in the queue has
    // position 0.
    public static void reverseHalf(Queue<Integer> q) {
        Stack<Integer> s = new Stack<Integer>();
        int size = q.size();
        for (int i = 0; i < size; i++) {
            int n = q.remove();
            if (i % 2 == 0)
                q.add(n);
            else
                s.push(n);
        }

        int newSize = q.size();
        for (int i = 0; i < newSize; i++) {
            q.add(q.remove());
            if (!s.isEmpty())
                q.add(s.pop());
        }
    }

    // isPalindrome accepts a queue of integers as a parameter and
    // returns true if the num- bers in the queue are the same in reverse order.
    public static boolean isPalindrome(Queue<Integer> q) {
        Stack<Integer> s = new Stack<Integer>();
        queueToStack(q, s);
        while (!s.isEmpty() && !q.isEmpty()) {
            int n = q.remove();
            q.add(n);
            if (s.pop() != n)
                return false;
        }
        return true;
    }

    // switchPairs accepts a stack of integers as a parameter and
    // swaps neighboring pairs of numbers starting at the bottom of the stack.
    public static void switchPairs(Stack<Integer> s) {
        Queue<Integer> q = new LinkedList<Integer>();
        stackToQueue(s, q);
        queueToStack(q, s);
        stackToQueue(s, q);
        int loop = q.size() / 2;
        for (int i = 0; i < loop; i++) {
            int n = q.remove();
            s.push(q.remove());
            s.push(n);
        }
        if (!q.isEmpty())
            s.push(q.remove());
    }

    // isConsecutive accepts a stack of integers as a parameter and
    // returns true if the stack contains a sequence of consecutive integers
    // starting from the bottom of the stack.
    // Consecutive integers are integers that come one after the other, as in 3, 4,
    // 5, etc.
    public static boolean isConsecutive(Stack<Integer> s) {
        Queue<Integer> q = new LinkedList<Integer>();
        stackToQueue(s, q);
        if (q.size() > 0)
            s.push(q.remove());
        while (!q.isEmpty()) {
            int n1 = s.peek();
            int n2 = q.remove();
            if ((n2 + 1) != n1)
                return false;
            else
                s.push(n2);
        }
        stackToQueue(s, q);
        queueToStack(q, s);
        return true;
    }

    // reorder accepts a queue of integers as a parameter
    // and that puts the integers into sorted (nondecreasing) order, assuming that
    // the queue is already sorted by absolute value.
    public static void reorder(Queue<Integer> q) {
        Stack<Integer> s = new Stack<Integer>();
        if (q.size() > 0)
            s.push(q.remove());
        while (!q.isEmpty()) {
            int n = q.remove();
            while (!s.isEmpty() && s.peek() > n)
                q.add(s.pop());
            s.push(n);
        }
        stackToQueue(s, q);
        queueToStack(q, s);
        stackToQueue(s, q);
    }

    // shift accepts a stack of integers and an integer n
    // as parameters and that shifts n values from the bottom of the stack to the
    // top of the stack.
    public static void shift(Stack<Integer> s, int n) {
        Queue<Integer> q = new LinkedList<Integer>();
        stackToQueue(s, q);
        queueToStack(q, s);
        stackToQueue(s, q);
        for (int i = 0; i < n; i++)
            q.add(q.remove());
        queueToStack(q, s);
    }

    // expunge that accepts a stack of integers as a parameter
    // and makes sure that the stack’s ele- ments are in nondecreasing order from
    // top to bottom, by removing from the stack any element that is smaller than
    // any element(s) on top of it.
    public static void expunge(Stack<Integer> s1) {
        Stack<Integer> s2 = new Stack<Integer>();
        if (s1.size() > 0)
            s2.push(s1.pop());
        while (!s1.isEmpty()) {
            int n = s1.pop();
            if (n >= s2.peek())
                s2.push(n);
        }

        while (!s2.isEmpty())
            s1.push(s2.pop());
    }

    // reverseFirstK accepts an integer k and a queue of
    // integers as parameters and reverses the order of the first k elements of the
    // queue, leaving the other elements in the same relative order.
    public static void reverseFirstK(int k, Queue<Integer> q) {
        if (k > q.size())
            throw new IllegalArgumentException();
        int minusK = q.size() - k;
        Stack<Integer> s = new Stack<Integer>();
        queueToStack(q, s);
        stackToQueue(s, q);
        for (int i = 1; i <= minusK; i++)
            s.push(q.remove());
        while (!s.isEmpty())
            q.add(s.pop());
    }

    // isSorted accepts a stack of integers as a
    // parameter and returns true if the elements in the stack occur in ascending
    // (nondecreasing) order from top to bottom. That is, the smallest element
    // should be on top, growing larger toward the bottom.
    public static boolean isSorted(Stack<Integer> s1) {
        Stack<Integer> s2 = new Stack<Integer>();
        boolean sorted = true;
        if (s1.size() > 0)
            s2.add(s1.pop());
        while (!s1.isEmpty()) {
            int n = s1.pop();
            if (n < s2.peek()) {
                sorted = false;
                s2.push(n);
                break;
            } else
                s2.push(n);
        }
        while (!s2.isEmpty())
            s1.push(s2.pop());
        return sorted;
    }

    // mirror accepts a stack of integers as a parameter
    // and replaces the stack contents with itself plus a mirrored version of itself
    // (the same elements in the opposite order).
    public static void mirror(Stack<Integer> s) {
        Queue<Integer> q = new LinkedList<Integer>();
        int size = s.size();
        stackToQueue(s, q);
        for (int i = 1; i <= size; i++) {
            int n = q.remove();
            s.push(n);
            q.add(n);
        }
        while (!s.isEmpty())
            q.add(s.pop());
        for (int i = 1; i <= size; i++)
            q.add(q.remove());
        queueToStack(q, s);
    }

    // compressDuplicates accepts a stack of integers as
    // a parameter and replaces each sequence of duplicates with a pair of
    // values: a count of the number of duplicates, followed by the actual
    // duplicated number.
    public static void compressDuplicates(Stack<Integer> s) {
        Queue<Integer> q = new LinkedList<Integer>();
        int currentValue = 0;
        int count = 0;
        int size = s.size();
        if (size > 0) {
            currentValue = s.pop();
            q.add(currentValue);
            count = 1;
        }
        while (!s.isEmpty()) {
            int n = s.pop();
            if (n == currentValue)
                count++;
            else if (n != currentValue) {
                q.add(count);
                q.add(n);
                count = 1;
                currentValue = n;
            }
        }
        q.add(count);
        queueToStack(q, s);
        stackToQueue(s, q);
        queueToStack(q, s);
        switchPairs(s);
    }

    // mirrorHalves accepts a queue of integers as a
    // parameter and replaces each half of that queue with itself plus a mirrored
    // version of itself (the same elements in the opposite order).
    public static void mirrorHalves(Queue<Integer> q) {
        int halfSize = q.size() / 2;
        if (q.size() % 2 != 0)
            throw new IllegalArgumentException();
        Stack<Integer> s = new Stack<Integer>();
        for (int i = 1; i <= 2; i++) {
            for (int j = 1; j <= halfSize; j++)
                s.push(q.remove());
            mirror(s);
            while (!s.isEmpty())
                q.add(s.pop());
        }
    }

    // removeMin accepts a stack of integers as a
    // parameter and removes and returns the small-
    // estvaluefromthestack
    public static int removeMin(Stack<Integer> s) {
        Queue<Integer> q = new LinkedList<Integer>();
        int min = s.pop();
        q.add(min);
        while (!s.isEmpty()) {
            int n = s.pop();
            q.add(n);
            if (n < min)
                min = n;
        }
        while (!q.isEmpty()) {
            int n = q.remove();
            if (n != min)
                s.push(n);
        }
        stackToQueue(s, q);
        queueToStack(q, s);
        return min;
    }

    // interleave accepts a queue of integers as a
    // parameter and rearranges the elements by alternating the elements from the
    // first half of the queue with those from the second half of the queue.
    public static void interleave(Queue<Integer> q) {
        if (q.size() % 2 != 0)
            throw new IllegalArgumentException();
        int halfSize = q.size() / 2;
        Stack<Integer> s = new Stack<Integer>();
        for (int i = 1; i <= halfSize; i++)
            s.push(q.remove());
        while (!s.isEmpty())
            q.add(s.pop());
        for (int i = 1; i <= halfSize; i++)
            q.add(q.remove());
        for (int i = 1; i <= halfSize; i++)
            s.push(q.remove());
        while (!s.isEmpty()) {
            q.add(s.pop());
            q.add(q.remove());
        }
    }
}