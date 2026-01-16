package JiEung2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.StringTokenizer;

public class BOJ10815 {
    public static int N, M;
    public static int[] arr;
    public int answer = 0;

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        N = Integer.parseInt(br.readLine());
        StringTokenizer st = new StringTokenizer(br.readLine());
        arr = new int[N];
        for (int i = 0; i < N; i++) {
            arr[i] = Integer.parseInt(st.nextToken());
        }
        M = Integer.parseInt(br.readLine());
        st = new StringTokenizer(br.readLine());
        StringBuilder sb = new StringBuilder();
        Arrays.sort(arr);
        for (int i = 0; i < M; i++) {
            sb.append(find(Integer.parseInt(st.nextToken())) + " ");
        }
        System.out.println(sb);
    }

    public static int find(int n) {
        int start = 0;
        int end = N - 1;
        while (start <= end) {
            int point = end + start / 2;

            if (n == arr[point]) {
                return 1;
            }

            if (n > arr[point]) {
                start = point + 1;
            } else if (n < arr[point]) {
                end = point - 1;
            }

        }
        return 0;
    }
}