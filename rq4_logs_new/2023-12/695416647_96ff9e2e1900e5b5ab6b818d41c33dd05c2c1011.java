import java.util.*;
import java.io.*;

public class BOJ_10974 {
    private static int n;
    private static int[] arr;
    private static boolean[] visited;

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        n = Integer.parseInt(br.readLine());
        arr = new int[n];
        visited = new boolean[n];

        for (int i = 0; i < arr.length; i++) {
            arr[i] = i + 1;
        }

        permutation(0);
    }

    public static void permutation(int depth) {
        if (depth == n) {
            print(arr);
            return;
        }

        for (int i = 0; i < n; i++) {
            if (!visited[i]) {
                arr[depth] = i + 1;
                visited[i] = true;
                permutation(depth + 1);
                visited[i] = false;
            }
        }
    }

    public static void print(int[] arr) {
        for (int i : arr) {
            System.out.print(i + " ");
        }
        System.out.println();
    }
}