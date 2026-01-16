package study.Baekjoon.month9_4;

import java.io.*;
import java.util.StringTokenizer;

/**
 * 문제이름 : 이항 계수 1
 * 링크 : https://www.acmicpc.net/problem/11050
 */

public class Baekjoon11050 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        StringTokenizer st;

        st = new StringTokenizer(br.readLine());

        int n = Integer.parseInt(st.nextToken());
        int k = Integer.parseInt(st.nextToken());

        bw.write(nCr(n, k) + "\n");
        bw.close();

    }

    public static int nCr(int n, int r) {
        if (n == r || r == 0)
            return 1;
        
        return nCr(n - 1, r - 1) + nCr(n - 1, r);
    }
}

/*


*/