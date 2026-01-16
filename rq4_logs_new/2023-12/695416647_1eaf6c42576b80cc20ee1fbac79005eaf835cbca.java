package PHS00;

import java.util.Scanner;

// 블랙잭
public class BOJ_2798 {
    public static void main(String[] args){
        Scanner scanner = new Scanner(System.in);
        int N = scanner.nextInt();
        int M = scanner.nextInt();

        int[] cards = new int[N];
        for(int i = 0; i < N; i++){
            cards[i] = scanner.nextInt();
        }

        int sum = 0;
        int answer = 0;
        for(int i = 0; i < N-2; i++){
            for(int j = i+1; j < N-1; j++){
                for(int k = j+1; k < N; k++){
                    sum = cards[i] + cards[j] + cards[k];
                    if(M-sum >= 0 && M-sum < M-answer){
                        answer = sum;
                    }
                    sum = 0;
                }
            }
        }
        System.out.println(answer);
    }
}