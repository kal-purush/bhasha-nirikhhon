import java.io.*;
import java.util.*;

public class Solution {

    public static void main(String[] args){
        Scanner sc = new Scanner(System.in);
        int T = sc.nextInt();
        int[] N = new int[T];
        for(int i = 0; i < T; i++){
            N[i] = sc.nextInt();
        }
        solve(N);
    }

    public static void solve(int[] N) {
        for (int i = 0; i < N.length; i++) {
            int n = N[i];
            long a = (n - 1) / 3;
            long b = (n - 1) / 5;
            long c = (n - 1) / 15;
            long d = a * (a + 1) / 2;
            long e = b * (b + 1) / 2;
            long f = c * (c + 1) / 2;
            long sum = 3 * d + 5 * e - 15 * f;
            System.out.println(sum);
        }
    }
}