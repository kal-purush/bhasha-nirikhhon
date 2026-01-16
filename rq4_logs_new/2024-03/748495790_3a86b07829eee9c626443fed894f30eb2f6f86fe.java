package main.week4.BOJ7453;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.StringTokenizer;

/**
 * 합이 0인 네 정수
 * 정렬, 투 포인터
 * @author hazel
 */
public class BOJ7453 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());

        int[] a = new int[n];
        int[] b = new int[n];
        int[] c = new int[n];
        int[] d = new int[n];

        for (int i = 0; i < n; i++) {
            StringTokenizer st = new StringTokenizer(br.readLine());
            a[i] = Integer.parseInt(st.nextToken());
            b[i] = Integer.parseInt(st.nextToken());
            c[i] = Integer.parseInt(st.nextToken());
            d[i] = Integer.parseInt(st.nextToken());
        }

        //배열 4개로 모든 경우의 수를 조사하면  4000^4 = 256,000,000,000,000로 시간초과 발생 가능성이 존재한다.
        //따라서 두개의 배열로 나눠서 연산
        long[] ab = new long[n * n]; // a와 b의 모든 요소를 각각 다 곱한 배열
        long[] cd = new long[n * n]; // c와 d의 모든 요소를 각각 다 곱한 배열

        int idx = 0;
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                ab[idx] = a[i] + b[j];
                cd[idx] = c[i] + d[j];
                idx++;
            }
        }

        //정렬 - 오름차순
        Arrays.sort(ab);
        Arrays.sort(cd);
        //System.out.println("ab : " + Arrays.toString(ab));
        //System.out.println("cd : " + Arrays.toString(cd));

        int leftPoint = 0; //ab의 인덱스
        int rightPoint = n * n - 1; //cd의 인덱스
        long cnt = 0;
        while (leftPoint < n * n && rightPoint >= 0) { //범위 조심하기

            long sum = ab[leftPoint] + cd[rightPoint];
            long leftCnt = 0;
            long rightCnt = 0;
            if (sum == 0) { //같을 때 모든 조합을 찾아야함.
                 /*
                    예를들어 ab = [-2,-1,-1,2], cd=[1,1,2,3]이라고 하면 총 경우의 수는 5이다.
                    단순히  (-2,2), (-1,1),(-1,1) 로 3가지라고 생각할 수 있지만
                    ab 배열에 -1이 2개, cd 배열에 1이 2개로 서로의 조합이 2 * 2로 가지의 경우의 수 4가지와 (-2,2) 1가지로 총 5가지이다.
                  */


                long abValue = ab[leftPoint]; //찾은 값
                long cdValue = cd[rightPoint];

                while (leftPoint < n * n && abValue == ab[leftPoint]) {
                    leftCnt++;
                    leftPoint++;
                }


                while (rightPoint >=0 && cd[rightPoint] == cdValue) {
                    rightCnt++;
                    rightPoint--;
                }
                cnt += leftCnt * rightCnt;

            } else if (sum > 0) {
                rightPoint--;
            } else {
                leftPoint++;
            }
        }

        System.out.println(cnt);
    }
}