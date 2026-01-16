package main.week5.BOJ2805;

import java.io.IOException;
import java.util.Scanner;

/**
 * 나무 자르기
 * https://www.acmicpc.net/problem/2805
 * 이진탐색
 *
 * @author hazel
 */
public class BOJ2805 {
    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);
        int n = scanner.nextInt(); //나무의 수. 최대값이 1,000,000
        int m = scanner.nextInt(); //집으로 가져가려는 나무 길이. 최대값이 2,000,000,000
        int[] arr = new int[n];

        //최대값 구하기 - 아무리 길게 절단해도 제일 긴 나무 길이만큼 절단 가능하기 때문
        int maxValue = Integer.MIN_VALUE;
        for (int i = 0; i < n; i++) {
            arr[i] = scanner.nextInt(); //최대값 1,000,000,000
            maxValue = Math.max(arr[i], maxValue);
        }

        int left = 0;
        int right = maxValue; // 예제1에서 범위를는 0 ~20
        while (left <= right) {
            int mid = (left + right) / 2;
            long treeLength = 0; //만약 나무의 높이가 1,000,000,000일때 누적값이 int의 범위를 넘을 수 있으므로 long
            for (int i = 0; i < n; i++) {
                if (arr[i] > mid) { //나무가 mid의 길이보다 길때만
                    treeLength += arr[i] - mid;
                }
            }

            if (treeLength >= m) { //나무 길이의 합을 더 늘려야함
                left = mid + 1;
            } else { //나무 길이의 합을 줄여아함.
                right = mid - 1;
            }
        }
        System.out.println(right);
    }
}