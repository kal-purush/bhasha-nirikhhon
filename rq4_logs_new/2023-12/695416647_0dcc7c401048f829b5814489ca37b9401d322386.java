package JiEung2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class BOJ2798 {
    static int[] check;
    static int answer = 0;
    static int min = Integer.MAX_VALUE;

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        int n = Integer.parseInt(st.nextToken());
        int m = Integer.parseInt(st.nextToken());
        check = new int[n];
        st = new StringTokenizer(br.readLine());
        int[] arr = new int[n];
        for (int i = 0; i < n; i++) {
            arr[i] = Integer.parseInt(st.nextToken());
        }
        for (int i = 0; i < n; i++) {
            check[i] = 1;
            DFS(m, 0, i, arr, arr[i]);
            check[i] = 0;
        }
        System.out.println(answer);
    }

    public static void DFS(int m, int L, int index, int[] arr, int sum) {
        if (L == 2) {
            if (m - sum >= 0 && m - sum < min) {
                answer = sum;
                min = m - sum;
            }
            return;
        }
        for (int i = index + 1; i < arr.length; i++) {
            if (check[i] == 0) {
                check[i] = 1;
                DFS(m, L + 1, i, arr, sum + arr[i]);
                check[i] = 0;
            }
        }
    }

}