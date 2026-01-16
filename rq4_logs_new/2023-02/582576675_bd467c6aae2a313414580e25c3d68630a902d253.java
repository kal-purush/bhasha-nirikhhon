import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * BOJ 1941: 소문난 칠공주
 * - 메모리: 87616 kb
 * - 시간: 440ms
 *
 * [풀이]
 * 1. 25명의 학생 중, 가능한 모든 학생 7명의 조합 중 조건에 맞는 것을 구한다.
 * 2. 구한 조합에 대해서 인접한 학생들인지 확인한다.
 *
 * [후기]
 * 첫 풀이로는 1) BFS로 인접한지 확인 2) 조건에 맞는지 확인하는 방식으로 풀었다. 이렇게 풀면, BFS가 T자 모양으로 있는 조합은 못 찾는다.
 * 그래서 반대로 1) 조건에 맞는 조합인지 확인 2) BFS로 인접한지 확인하는 방식으로 풀었더니 풀렸다.
 *
 * 위에 짧게 적었지만 사실 너무 어려웠다...
 * T자 모양 못 찾는 것도 찾기 어려웠고, 조합 구할 때는 0~24 사이 중 7개를 고르는 형식으로 했는데 이걸 1차원<=>2차원 배열을 어떻게 변환할지 또 고민했다.
 * 중간에 BFS 조건 잘못 정해서 모든 경우의 수를 다 맞다고 한 것도 있고... 여러모로 다시 풀어볼 만한 문제라고 느꼈다.
 * 사실 이거 한 줄로 연결되어있는 거라 DFS가 더 빠를 거 같긴 하다. 다음번에 실험해보는 걸로.
 */


public class BOJ_1941 {

    static int count = 0; // 경우의 수

    static int[] dr = {-1, 1, 0, 0};
    static int[] dc = {0, 0, -1, 1};

    static char[][] classMap = new char[5][]; // 학생 배치
    static boolean[] students = new boolean[25]; // 선택된 학생의 인덱스=true로 조합 구할 배열

    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        for (int r = 0; r < 5; ++r) {
            classMap[r] = br.readLine().trim().toCharArray();
        }

        getStudentCombo(0, 0, 0);

        System.out.println(count);
    }

    // 0~24번까지 7명의 학생을 선택할 수 있는 모든 조합 중 조건에 맞는 것 구하기
    static void getStudentCombo(int start, int depth, int lCount) {
        if (depth == 7) {
            if (lCount >= 4) { // 만약 이다솜파 학생이 4명 이상이라면
                if (checkConnection(students)) { // 연결된 자리인지 확인
                    ++count;
                }
            }
            return;
        }

        for (int i = start; i < 25; ++i) {
            students[i] = true;
            if (classMap[i / 5][i % 5] == 'S') { // 만약 이다솜파 학생이라면
                getStudentCombo(i + 1, depth + 1, lCount + 1);
            } else {
                getStudentCombo(i + 1, depth + 1, lCount);
            }
            students[i] = false;
        }
    }

    // 구한 학생 조합에 대해 연결된 자리인지 자리인지 확인
    static boolean checkConnection(boolean[] students) {

        Queue<Integer> queue = new ArrayDeque<>();
        boolean[] visited = new boolean[25];
        
        for (int i = 0; i < 25; ++i) { // BFS 시작할 자리 구하기
            if (students[i]) {
                queue.offer(i);
                visited[i] = true;
                break;
            }
        }

        int count = 1; // 인접한 학생 수 
        while (!queue.isEmpty()) {
            Integer curr = queue.poll();

            for (int d = 0; d < 4; ++d) {
                int nr = curr / 5 + dr[d];
                int nc = curr % 5 + dc[d];

                // 범위 내에 있고, 방문하지 않았고, 선택된 학생 중 한 명이라면
                if (inRange(nr, nc) && !visited[nr * 5 + nc] && students[nr * 5 + nc]) {
                    ++count;
                    queue.offer(nr * 5 + nc);
                    visited[nr * 5 + nc] = true;
                }
            }
        }

        return count == 7;
    }

    static boolean inRange(int r, int c) {
        return r >= 0 && c >= 0 && r < 5 && c < 5;
    }
}