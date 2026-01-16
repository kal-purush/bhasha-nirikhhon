package IntroductionToDataStructure.Day09;

import java.util.Deque;
import java.util.LinkedList;

// 232.用栈实现队列
public class ImplementQueueUsingStacks232 {
    public static void main(String[] args) {
        MyQueue myQueue = new MyQueue();
        myQueue.push(1);
        myQueue.push(2);
        System.out.println(myQueue.pop());
        System.out.println(myQueue.peek());
    }

    static class MyQueue {
        // 双端队列 模拟 栈
        // inStack 表示输入栈，用于压入 push 传入的数据
        // outStack 表示输出栈，用于 pop 和 peek 操作
        Deque<Integer> inStack;
        Deque<Integer> outStack;

        // 初始化队列，new 两个 LinkedList 表示 栈
        /** Initialize your data structure here. */
        public MyQueue() {
            inStack = new LinkedList<>();
            outStack = new LinkedList<>();
        }

        // 队列 push 操作，输入栈 push 即可
        /** Push element x to the back of queue. */
        public void push(int x) {
            inStack.push(x);
        }

        // 队列弹出并返回顶端元素的方法
        /** Removes the element from in front of queue and returns that element. */
        public int pop() {
            // 每次弹出前，如果输出栈为空，将输入栈的弹出的数据压入输出栈
            if (outStack.isEmpty()) {
                in2out();
            }
            // 输出栈弹出数据
            return outStack.pop();
        }

        // 队列返回顶端元素的方法
        /** Get the front element. */
        public int peek() {
            // 每次返回元素前，如果输出栈为空，将输入栈的弹出的数据压入输出栈
            if (outStack.isEmpty()) {
                in2out();
            }
            // 输出栈返回顶端元素
            return outStack.peek();
        }

        // 队列判空操作，当输入栈和输出栈都为空时，队列为空
        /** Returns whether the queue is empty. */
        public boolean empty() {
            return inStack.isEmpty() && outStack.isEmpty();
        }

        // 定义 输入栈 向 输出栈 传数据的方法
        // 每次 pop 或 peek 时，若输出栈为空，则将输入栈的全部数据依次弹出并压入输出栈，
        // 这样输出栈从栈顶往栈底的顺序就是队列从队首往队尾的顺序。
        public void in2out() {
            // 循环截止条件为 输入栈不为空，也即 当输入栈为空时，循环停止
            while (!inStack.isEmpty()) {
                // 输出栈压入输入栈弹出的数据
                outStack.push(inStack.pop());
            }
        }
    }

/**
 * Your MyQueue object will be instantiated and called as such:
 * MyQueue obj = new MyQueue();
 * obj.push(x);
 * int param_2 = obj.pop();
 * int param_3 = obj.peek();
 * boolean param_4 = obj.empty();
 */
}