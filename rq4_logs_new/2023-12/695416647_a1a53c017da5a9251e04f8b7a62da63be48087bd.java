package PHS00;

import java.util.Scanner;

// 모든 순열
public class BOJ_10974 {
    static public boolean[] visit;
    static public int N;
    static public int[] arr;
    public static void main(String[] args){

        Scanner scanner = new Scanner(System.in);

        N = scanner.nextInt();
        visit = new boolean[N+1];
        arr = new int[N];
        permutation(0);

    }
    private static void permutation(int cnt){
        if(cnt == N){
            for(int i = 0; i < arr.length; i++){
                System.out.print(arr[i] + " ");
            }
            System.out.println();
            return;
        }
        for(int i = 1; i <= N; i++){
            if(!visit[i]){
                visit[i] = true;
                arr[cnt] = i;
                permutation(cnt+1);
                visit[i] = false;
            }
        }
    }
}