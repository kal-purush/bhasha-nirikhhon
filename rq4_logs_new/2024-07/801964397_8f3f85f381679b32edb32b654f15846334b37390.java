package week07.Step01_20444;
/*
첫 줄에 정확히 n번의 가위질로 k개의 색종이 조각을 만들 수 있다면 YES,
아니라면 NO를 출력한다.
---
4 9
YES
---

* */

import java.io.*;
import java.util.*;

public class Main {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        StringBuilder sb = new StringBuilder();

        long n = Long.parseLong(st.nextToken());
        long k = Long.parseLong(st.nextToken());

        long left = 0;
        long right = n / 2;
        long mid;
        long piece;

        boolean result = false;

        while (left <= right) {
//            mid = (left + right); // 가운데는 가로로 자른 횟수
//            piece = (mid + 1) * ((n-mid)+1); // 조각의 개수
            long row = (left + right) / 2;
            long col = n - row;

            long total = cut_paper(row, col);
            if (total == k) {
                System.out.println("YES");
                return;
            } else if (total > k) { //row col의 차이가 더 커야한다.
                right = row - 1;
            } else if (total < k) {
                left = row + 1;
            }
        }
        System.out.println("NO");
    }
    public static long cut_paper(long row, long col) {
        return (row + 1) * (col + 1);
    }
}

