package PHS00;

import java.util.Scanner;

// 로또
public class BOJ_6603 {
    private static Scanner scanner;
    private static int[] nums;
    private static boolean[] visit;
    private static int[] lotto = new int[6];
    public static void main(String[] args){
        scanner = new Scanner(System.in);

        while(true){
            int N = scanner.nextInt();
            if(N == 0){
                break;
            }
            nums = new int[N];
            visit = new boolean[N];
            for(int i = 0; i < nums.length; i++){
                nums[i] = scanner.nextInt();
            }
            combination(0, 0);
            System.out.println();
        }

    }
    private static void combination(int cnt, int start){
        if(cnt == 6){
            for(int num : lotto){
                System.out.print(num + " ");
            }
            System.out.println();
            return;
        }
        for(int i = start; i < nums.length; i++){
            if(!visit[i]){
                visit[i] = true;
                lotto[cnt] = nums[i];
                combination(cnt+1, i+1);
                visit[i] = false;
            }
        }
    }
}