package DoIt.Stack;

import java.io.IOException;
import java.util.Scanner;
import java.util.Stack;

/*
    012 오큰수 구하기
    스택, 인덱스
 */
public class Solution2_2 {
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        int[] num = new int[n];
        Stack<Integer> stack = new Stack();
        for(int i = 0; i < n; i++){
            num[i] = sc.nextInt();
        }
        stack.push(0);
        int[] answer = new int[n];
        for(int i = 1; i < n; i++){
            while(!stack.isEmpty() && num[stack.peek()] < num[i])
                answer[stack.pop()] = num[i];
            stack.push(i);
        }
        while(!stack.empty()){
            answer[stack.pop()] = -1;
        }
        for(int i = 0; i < n; i++){
            System.out.print(answer[i] + " ");
        }
    }
}