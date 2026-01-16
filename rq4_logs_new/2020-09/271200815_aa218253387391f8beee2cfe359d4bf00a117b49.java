package com.practice.hackerRank.queue;

import java.io.*;
import java.util.ArrayDeque;
import java.util.Deque;

public class WindowMaximum {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        int test = Integer.parseInt(br.readLine());
        while (test > 0){
            String[] size = br.readLine().split(" ");
            int n = Integer.parseInt(size[0]);
            int k = Integer.parseInt(size[1]);
            int[] arr = new int[n];
            String[] sArr = br.readLine().split(" ");
            for (int i = 0; i < n; i++)
                arr[i] = Integer.parseInt(sArr[i]);
            int ans = windowMaximum(arr, n, k);
            bw.write(ans + "");
            bw.newLine();
            test--;
        }
        bw.flush();
    }

    private static int windowMaximum(int[] arr, int n, int k) {
        int sum = 0;
        Deque<Integer> deque = new ArrayDeque<>();
        for (int i = 0; i < n; i++){
            while (!deque.isEmpty() && arr[deque.getLast()] <= arr[i])
                deque.removeLast();
            deque.addLast(i);
            if (deque.getFirst() == i-k)
                deque.removeFirst();
            if (i >= k - 1)
                sum += arr[deque.getFirst()];
        }
        return sum;
    }
}