package com.practice.hackerRank.queue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;


public class ImplementDeQue {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int test = Integer.parseInt(br.readLine());
        DeQues deQues = new DeQues();
        while (test > 0) {
            String s = br.readLine();
            String[] arr = s.split(" ");
            if (arr[0].equals("pop_front"))
                System.out.println(deQues.popFront());
            else if (arr[0].equals("pop_back"))
                System.out.println(deQues.popBack());
            else if (arr[0].equals("push_front"))
                deQues.pushFront(Integer.parseInt(arr[1]));
            else
                deQues.pushBack(Integer.parseInt(arr[1]));
            test--;
        }

    }
}

class DeQues{
    ArrayList<Integer> deque;
    int front;
    int rear;
    public DeQues(){
        deque = new ArrayList<>();
        front = 0;
        rear = -1;
    }

    void pushFront(int x){
        deque.add(0, x);
        front = 0;
    }

    public void pushBack(int x) {
        rear++;
        deque.add(x);
    }

    public String popBack() {
        if (rear < front)
            return "Empty";
        int n = deque.get(rear);
        deque.remove(rear);
        rear--;
        return Integer.toString(n);
    }

    public String popFront() {
        if (rear < front)
            return "Empty";
        int n = deque.get(0);
        deque.remove(0);
        front++;
        return Integer.toString(n);
    }
}