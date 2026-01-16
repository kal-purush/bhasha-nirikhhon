package ybwi0912;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Stack;
import java.util.StringTokenizer;

/*
* BOJ 2841번: 외계인의 기타 연주
* [ 문제 ]
* 보통 기타는 1번 줄부터 6번 줄까지 총 6개의 줄이 있고, 각 줄은 P개의 프렛으로 나누어져 있다. 프렛의 번호도 1번부터 P번까지 나누어져 있다.
* 손가락으로 프렛을 한 번 누르거나 떼는 것을 손가락을 한 번 움직였다고 센다.
* 어떤 멜로디가 주어졌을 때, 손가락의 가장 적게 움직이는 회수를 구하는 프로그램을 작성하시오.
*
* [ 제한사항 ]
* 1 <= N <= 500,000
* 2 <= P <= 300,000
*
* [ 입력 ]
* 첫째 줄에 멜로디에 포함되어 있는 음의 수 N과 한 줄에 있는 프렛의 수 P가 주어진다.
* 다음 N개의 줄에 멜로디의 한 음을 나타내는 두 정수가 주어진다.
* 첫 번째 정수는 줄의 번호이고 두 번째 정수는 그 줄에서 눌러야 하는 프렛의 번호이다.
*
* [ 입출력 예 ]
* -- 예제 입력 1 --
* 5 15
* 2 8
* 2 10
* 2 12
* 2 10
* 2 5
* -- 예제 출력 1 --
* 7
* -- 예제 입력 2 --
* 7 15
* 1 5
* 2 3
* 2 5
* 2 7
* 2 4
* 1 5
* 1 3
* -- 예제 출력 2 --
* 9
* */

public class BOJ2841 {
    public static void main(String[] args) throws IOException {
        BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer token = new StringTokenizer(bf.readLine());
        int N = Integer.parseInt(token.nextToken()); // 멜로디에 포함되어 있는 음의 수
        int P = Integer.parseInt(token.nextToken()); // 한 줄에 있는 프렛의 수

        Stack<Integer>[] strings = new Stack[6];
        for(int i=0; i<6; i++){
            strings[i] = new Stack();
        }

        int count = 0;

        for(int i=0; i<N; i++){
            token = new StringTokenizer(bf.readLine());
            int line = Integer.parseInt(token.nextToken()) - 1;
            int fret = Integer.parseInt(token.nextToken());

            while (!strings[line].isEmpty() && strings[line].peek() > fret) {
                strings[line].pop();
                count++;
            } // fret을 누른 음을 연주하기 위해 fret보다 큰 프렛을 누르고 있는 손가락을 모두 떼어야 한다

            // 1. fret보다 작은 프렛을 누르고 있는 손가락이 존재한다면 굳이 손가락을 떼지 않고 fret을 눌러도 된다
            // 2. 해당 줄을 누르고 있는 손가락이 없다면 바로 fret을 눌러도 된다
            if ((!strings[line].isEmpty() && strings[line].peek() < fret) || strings[line].empty()) {
                strings[line].add(fret);
                count++;
            }
        }

        System.out.println(count);
    }
}