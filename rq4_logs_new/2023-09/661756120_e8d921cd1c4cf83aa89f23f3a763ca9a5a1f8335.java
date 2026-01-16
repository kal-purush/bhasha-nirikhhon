/*
3184. Silver 1 - 양

    시간 제한	    메모리 제한        제출        정답	      맞힌 사람	    정답 비율
    1 초	    128 MB           9726	    6051      4763	         63.245%


    문제
        미키의 뒷마당에는 특정 수의 양이 있다. 그가 푹 잠든 사이에 배고픈 늑대는 마당에 들어와 양을 공격했다.
        마당은 행과 열로 이루어진 직사각형 모양이다. 글자 '.' (점)은 빈 필드를 의미하며, 글자 '#'는 울타리를, 'o'는 양, 'v'는 늑대를 의미한다.
        한 칸에서 수평, 수직만으로 이동하며 울타리를 지나지 않고 다른 칸으로 이동할 수 있다면, 두 칸은 같은 영역 안에 속해 있다고 한다. 마당에서 "탈출"할 수 있는 칸은 어떤 영역에도 속하지 않는다고 간주한다.
        다행히 우리의 양은 늑대에게 싸움을 걸 수 있고 영역 안의 양의 수가 늑대의 수보다 많다면 이기고, 늑대를 우리에서 쫓아낸다. 그렇지 않다면 늑대가 그 지역 안의 모든 양을 먹는다.
        맨 처음 모든 양과 늑대는 마당 안 영역에 존재한다.
        아침이 도달했을 때 살아남은 양과 늑대의 수를 출력하는 프로그램을 작성하라.


    입력
        첫 줄에는 두 정수 R과 C가 주어지며(3 ≤ R, C ≤ 250), 각 수는 마당의 행과 열의 수를 의미한다.
        다음 R개의 줄은 C개의 글자를 가진다. 이들은 마당의 구조(울타리, 양, 늑대의 위치)를 의미한다.


    출력
        하나의 줄에 아침까지 살아있는 양과 늑대의 수를 의미하는 두 정수를 출력한다.


    예제 입력 1
        6 6
        ...#..
        .##v#.
        #v.#.#
        #.o#.#
        .###.#
        ...###
    예제 출력 1
        0 2

    예제 입력 2
        8 8
        .######.
        #..o...#
        #.####.#
        #.#v.#.#
        #.#.o#o#
        #o.##..#
        #.v..v.#
        .######.
    예제 출력 2
        3 1

    예제 입력 3
        9 12
        .###.#####..
        #.oo#...#v#.
        #..o#.#.#.#.
        #..##o#...#.
        #.#v#o###.#.
        #..#v#....#.
        #...v#v####.
        .####.#vv.o#
        .......####.
    예제 출력 3
        3 5


    알고리즘 분류
        그래프 이론
        그래프 탐색
        너비 우선 탐색
        깊이 우선 탐색
*/


// 메모리 : 17888KB
// 시간 : 216ms
// 코드 길이 : 4950B
// 정답

package C0012S;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.StringTokenizer;

public class BOJ3184_1 {
    static int R; // 마당의 행의 수 (3 ≤ R ≤ 250)
    static int C; // 마당의 열의 수 (3 ≤ C ≤ 250)
    static char garden[][]; // 마당의 구조를 저장하는 2 차원 배열
    static int dx[] = {-1, 1, 0, 0}; // 상, 하, 좌, 우
    static int dy[] = {0, 0, -1, 1}; // 상, 하, 좌, 우
    static int sheepNum; // 영역 안의 양의 수
    static int wolfNum; // 영역 안의 늑대의 수

    public static boolean check(int x, int y) { // 좌표가 마당 내의 좌표인지 검사하는 함수  // 마당 내의 좌표일 경우 true, 마당 내의 좌표가 아닐 경우 false 반환
        if (x >= 0 && x < R && y >= 0 && y < C) {
            return true;
        }

        return false;
    }

    public static void findAnimalNum(int x, int y) { // 영역 안의 양의 수와 영역 안의 늑대의 수를 구하는 함수  // BFS Algorithm 적용
        sheepNum = 0;
        wolfNum = 0;

        Queue<int[]> queue = new ArrayDeque<>(); // 검사할 지점의 좌표를 저장하는 큐

        queue.offer(new int[] {x, y}); // 검사를 시작할 지점의 좌표를 큐에 추가
        if (garden[x][y] == 'o') { // 검사를 시작할 지점의 좌표에 양이 있을 경우
            sheepNum += 1; // 영역 안의 양의 수 추가
        }
        else if (garden[x][y] == 'v') { // 검사를 시작할 지점의 좌표에 늑대가 있을 경우
            wolfNum += 1; // 영역 안의 늑대의 수 추가
        }
        garden[x][y] = '#'; // 검사를 시작할 지점의 좌표의 마당의 구조를 울타리로 변경
        while (!queue.isEmpty()) {
            int now[] = queue.poll(); // 검사할 지점의 좌표

            for (int d = 0; d < 4; d++) { // 상, 하, 좌, 우 방향에 있는 다음 지점 체크
                int nx = now[0] + dx[d]; // 다음 지점의 행의 인덱스
                int ny = now[1] + dy[d]; // 다음 지점의 열의 인덱스

                if (check(nx, ny) && garden[nx][ny] != '#') { // 다음 지점이 마당의 범위를 벗어나지 않았고, 다음 지점에 울타리가 있지 않을 경우
                    if (garden[nx][ny] == 'o') { // 다음 지점에 양이 있을 경우
                        sheepNum += 1; // 영역 안의 양의 수 추가
                    }
                    else if (garden[nx][ny] == 'v') { // 다음 지점에 늑대가 있을 경우
                        wolfNum += 1; // 영역 안의 늑대의 수 추가
                    }

                    garden[nx][ny] = '#'; // 다음 지점을 검사했으므로 해당 좌표의 마당의 구조를 울타리로 변경

                    queue.offer(new int[] {nx, ny}); // 다음 지점을 검사할 지점의 좌표 큐에 추가
                }
            }
        }

        if (sheepNum > wolfNum) { // 영역 안의 양의 수가 영역 안의 늑대의 수보다 많을 경우
            wolfNum = 0; // 늑대를 우리에서 쫓아낸다.
        }
        else { // 영역 안의 양의 수가 영역 안의 늑대의 수보다 많지 않을 경우
            sheepNum = 0; // 양은 늑대에게 잡아먹힌다.
        }
    }

    public static void calculateAnimalNum() { // 아침이 도달했을 때 살아남은 양과 늑대의 수를 구하고 출력하는 함수
        int tomorrowSheepNum = 0; // 아침이 도달했을 때 살아남은 양의 수
        int tomorrowWolfNum = 0; // 아침이 도달했을 때 살아남은 늑대의 수

        for (int r = 0; r < R; r++) {
            for (int c = 0; c < C; c++) {
                if (garden[r][c] != '#') { // 울타리가 아닐 경우
                    findAnimalNum(r, c); // 해당 좌표가 속해 있는 영역을 검사

                    tomorrowSheepNum += sheepNum; // 아침이 도달했을 때 살아남은 양의 수에 해당 영역의 양의 수 추가
                    tomorrowWolfNum += wolfNum; // 아침이 도달했을 때 살아남은 늑대의 수에 해당 영역의 늑대의 수 추가
                }
            }
        }

        System.out.println(tomorrowSheepNum + " " + tomorrowWolfNum);
    }

    public static void main(String[] args) throws IOException {
        BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer token = new StringTokenizer(bf.readLine());
        R = Integer.parseInt(token.nextToken());
        C = Integer.parseInt(token.nextToken());

        garden = new char[R][C];
        for (int i = 0; i < R; i++) {
            String state = bf.readLine();

            for (int j = 0; j < C; j++) {
                garden[i][j] = state.charAt(j);
            }
        }

        calculateAnimalNum();
    }
}