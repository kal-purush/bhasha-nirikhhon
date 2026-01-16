package Jieun714;
/**
 * 문제: 세 개의 장대가 있고 첫 번째 장대에는 반경이 서로 다른 n개의 원판이 쌓여 있다. 각 원판은 반경이 큰 순서대로 쌓여있다. 이제 수도승들이 다음 규칙에 따라 첫 번째 장대에서 세 번째 장대로 옮기려 한다.
 *      1. 한 번에 한 개의 원판만을 다른 탑으로 옮길 수 있다.
 *      2. 쌓아 놓은 원판은 항상 위의 것이 아래의 것보다 작아야 한다.
 *      이 작업을 수행하는데 필요한 이동 순서를 출력하는 프로그램을 작성하라. 단, 이동 횟수는 최소가 되어야 한다.
 * 입력: 첫째 줄에 첫 번째 장대에 쌓인 원판의 개수 N (1 ≤ N ≤ 20)이 주어진다.
 * 출력: 첫째 줄에 옮긴 횟수 K를 출력한다.
 *      두 번째 줄부터 수행 과정을 출력한다. 두 번째 줄부터 K개의 줄에 걸쳐 두 정수 A B를 빈칸을 사이에 두고 출력하는데,
 *      이는 A번째 탑의 가장 위에 있는 원판을 B번째 탑의 가장 위로 옮긴다는 뜻이다.
 *
 * 해결: 재귀. 하노이탑 이동 횟수 공식: 2^n-1
 * 참고: https://st-lab.tistory.com/96
 * */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class BOJ11729 {
    public static int N;
    public static StringBuilder sb = new StringBuilder();

    public static void hanoi(int N, int start, int mid, int end){
        if(N == 1){ //이동할 원판이 하나일 때
            sb.append(start + " " + end + "\n");
            return;
        }
        // N-1개를 start에서 mid로 이동
        hanoi(N - 1, start, end, mid);

        // start 지점의 N번째 원판을 to지점으로
        sb.append(start + " " + end + "\n");

        // mid 지점의 N-1개의 원판을 to 지점으로
        hanoi(N - 1, mid, start, end);
    }

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        N = Integer.parseInt(br.readLine()); //원판의 개수

        sb.append((int) Math.pow(2, N) -1 +"\n"); //하노이탑 옮긴 횟수: 2^n-1
        hanoi(N, 1, 2, 3);
        System.out.println(sb); //결과 출력
    }
}