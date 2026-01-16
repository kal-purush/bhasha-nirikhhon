package LeetCode_Medium;

import nodes.MyQueue;

import java.util.Stack;

public class test {
    public static void main(String[] args) {
        Stack<Integer> stack = new Stack<>();
        MyQueue myQueue = new MyQueue();

//        stack.push(1);
//        stack.push(2);
//        stack.push(3);

        myQueue.push(1);
        myQueue.push(2);
        myQueue.push(3);
        myQueue.push(4);
        myQueue.push(5);

        System.out.println(myQueue.getInputStacksize());

        myQueue.peek();

        System.out.println(myQueue.getInputStacksize());




    }
}