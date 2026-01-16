/**
 * 아이디어 : 사다리를 거꾸로 뒤집어서 X에서 출발지를 찾아감
 * 메모리 : 134 ms
 * 시간 : 35,840 kb
 * 난이도 : 상 (이유는 모르겠지만 계속 답이 다르게 나오거나 무한 루프가 돌아서 어려웠습니다.)
 */

import java.io.*;
import java.util.*;

public class Solution {
    static int SIZE = 100;          // 보드판 가로 세로 길이
    static int xx, xy;              // 보드판 X 좌표
    static int [][] map;            // 보드판

    // X에서 시작하여 출발점 X 좌표를 찾는 코드
    static int findStart() {
        int row = xx;   // row에 X의 x좌표 대입
        int col = xy;   // col에 X의 y좌표 대입

        do {
            map[col][row] = 7;     // 없으면 무한루프

            // 왼쪽으로 이동할 수 있으면 왼쪽으로 이동
            if (row != 0 && map[col][row - 1] == 1) {
                row--;
            }
            // 오른쪽으로 이동할 수 있으면 오른쪽으로 이동
            else if (row != SIZE - 1 && map[col][row + 1] == 1) {
                row++;
            }
            // 좌우로 이동이 불가하면 위로 이동
            else {
                col--;
            }
        } while (col != 0); // 출발점에 도착하면 반복문 종료

        return row; // 출발점의 x 좌표 반환
    }

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringBuilder result = new StringBuilder();     // 결과 저장을 위한 StringBuilder 선언

        for (int i = 0; i < 1; i++) {
            int T = Integer.parseInt(br.readLine());    // 테스트 케이스 번호를 입력받음
            map = new int[SIZE][SIZE];                  // 보드판 초기화

            // 보드판에 사다리 정보를 입력받음
            for (int j = 0; j < SIZE; j++) {
                StringTokenizer row = new StringTokenizer(br.readLine());

                for (int k = 0; k < SIZE; k++) {
                    int value = Integer.parseInt(row.nextToken());
                    map[j][k] = value;

                    // 도착점의 좌표도 동시에 찾음
                    if (value == 2) {
                        xx = k;
                        xy = SIZE-1;
                    }
                }
            }

            // 도착점의 좌표를 찾아서 start에 저장
            int start = findStart();

            // 결과를 result에 저장
            result.append("#").append(T).append(" ").append(start).append("\n");
        }

        // 결과 출력
        System.out.println(result);
    }
}
/**
 * 아이디어 : 달팽이 모양으로 움직일 때마다 움직인 공간의 차원을 축소시킴
 * 시간 : 92 ms
 * 메모리 : 25216 kb
 * 난이도 : 하 (전에 풀었던 문제라 금방 인덱스 조작을 위한 패턴을 찾을 수 있었습니다.)
 */

import java.io.*;

public class Solution {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringBuilder result = new StringBuilder(); // 결과를 저장할 StringBuilder
        int T = Integer.parseInt(br.readLine());    // 테스트 케이스 개수를 저장할 변수

        // 테스트 케이스 개수만큼 반복문을 돌며 테스트 케이스를 처리
        for (int i = 1; i <= T; i++) {
            int N = Integer.parseInt(br.readLine());    // N을 입력받음

            // 차원 축소를 위한 변수 선언
            int rowStart = 0;   // 가로 공간 시작 인덱스
            int colStart = 0;   // 세로 공간 시작 인덱스
            int rowEnd = N-1;   // 가로 공간 끝 인덱스
            int colEnd = N-1;   // 새로 공간 끝 인덱스

            int num = 1;    // 달팽이 배열에 숫자를 채워넣기 위한 변수
            int [][] map = new int[N][N];   // 달팽이 보드판

            // 달팽이 모양으로 숫자를 채워넣기 위해 N * N번 반복문을 돌림
            while (num < N*N+1) {
                // 오른쪽 방향 탐색
                for (int j = rowStart; j <= rowEnd; j++) {
                    map[colStart][j] = num++;
                }
                colStart++; // 가로 위쪽 공간 축소

                // 아래 방향 탐색
                for (int j = colStart; j <= colEnd; j++) {
                    map[j][rowEnd] = num++;
                }
                rowEnd--;   // 세로 오른쪽 공간 축소

                // 왼쪽 방향 탐색
                for (int j = rowEnd; j >= rowStart; j--) {
                    map[colEnd][j] = num++;
                }
                colEnd--;   // 가로 아래쪽 공간 축소

                // 위쪽 방향 탐색
                for (int j = colEnd; j >= colStart; j--) {
                    map[j][rowStart] = num++;
                }
                rowStart++; // 세로 왼쪽 공간 축소
            }

            // 결과를 result에 저장
            result.append("#").append(i).append("\n");
            for (int j = 0; j < N; j++) {
                for (int k = 0; k < N; k++) {
                    result.append(map[j][k]).append(" ");
                }
                result.append("\n");
            }
        }

        System.out.println(result); // 결과 출력
    }
}
/**
 * 아이디어 : 인영이의 카드의 경우의 수가 9!로 10!이 안되고
 * 카드만 결정하면 카드를 하나씩 비교해가며 승패를 결정하면 되기 때문에,
 * 완전 탐색으로 문제를 해결할 수 있을거 같습니다.
 * 시간 : 2622 ms
 * 메모리 : 26916 kb
 * 난이도 : 중 (완전 탐색을 이해하고 나니 코드를 설계해서 한번에 통과할 수 있었습니다.)
 */

import java.io.*;
import java.util.*;

public class Solution {
    static int T;
    static boolean [] visited;
    static int [] nums;
    static int [] gNums;
    static int [] iNums;
    static int win, lose;

    // 인영이의 카드 패를 만듦
    static void makeINums() {
        boolean [] check = new boolean[19]; // 규영이가 가지고 있지 않는 숫자를 체크하기 위한 변수

        // 1~18 중 규영이가 가지고 있지 않은 숫자를 체크
        for (int i = 0; i < 9; i++) {
            check[gNums[i]] = true;
        }

        // 규영이가 가지고 있지 않는 수자로 인영이의 패를 만듦
        int index = 0;
        for (int i = 1; i < 19; i++) {
            if (!check[i])
                iNums[index++] = i;
        }
    }

    // 인영이와 규영이의 카드패를 비교하여 승패를 결정하는 함수
    static void battle() {
        int gScore = 0; // 규영이의 점수를 저장하기 위한 변수
        int iScore = 0; // 인영이의 점수를 저장하기 위한 변수

        // 패의 카드를 각각 한장씩 비교해가면서 점수를 합산
        for (int i = 0; i < 9; i++) {
            // 규영이의 카드가 더 숫자가 크면
            if (gNums[i] > nums[i])
                // 규영이 점수에 플러스
                gScore += gNums[i] + nums[i];
                // 인영이의 카드가 더 숫자가 크면
            else if (nums[i] > gNums[i])
                // 인영이의 점수에 플러스
                iScore += gNums[i] + nums[i];
        }

        // 규영이의 점수가 더 크면
        if (gScore > iScore)
            win++;  // 승리 횟수 증가
            // 인영이의 점수가 더 크면
        else if (iScore > gScore)
            lose++; // 패배 횟수 증가
    }

    // 순열 함수
    static void permutation(int depth) {
        // 9개의 카드 패를 모두 결정하였으면 재귀 호출 종료
        if (depth == 9) {
            // 결정된 9개의 카드패를 통해 승패를 결정
            battle();
            // 리턴
            return;
        }

        // 9장의 카드패를 결정하기 위해 반복문을 돌림
        for (int i = 0; i < 9; i++) {
            // 이미 선택한 카드이면 continue
            if (visited[i]) continue;

            visited[i] = true;  // 해당 카드를 이미 뽑았다고 체크
            nums[depth] = iNums[i]; // 카드 패에 현재 카드를 저장

            permutation(depth + 1); // 다음 카드를 결정하기 위해 재귀 호출

            nums[depth] = 0;    // 리셋
            visited[i] = false; // 해당 카드를 뽑았다는 표시를 원복
        }
    }

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        T = Integer.parseInt(br.readLine());    // 테스트 케이스 개수를 입력받음
        StringBuilder result = new StringBuilder(); // 결과를 저장할 StringBuilder

        // 반복문을 돌며 테스트 케이스를 입력받고 처리
        for (int i = 1; i <= T; i++) {
            visited = new boolean[9];   // 해당 번호를 뽑았는지 안 뽑았는지 체크하기 위한 변수
            nums = new int[9];          // 인영이의 패를 저장할 변수
            gNums = new int[9];         // 규영이의 패를 저장할 변수
            iNums = new int[9];         // 인영이의 패가 될 수 있는 숫자들을 저장한 배열
            win = 0;                    // 규영이의 승리 횟수를 저장할 변수
            lose = 0;                   // 규영이의 패배 횟수를 저장할 변수

            // 규영이의 패를 입력받음
            StringTokenizer cards = new StringTokenizer(br.readLine());

            // 규영이의 패를 초기화
            for (int j = 0; j < 9; j++) {
                gNums[j] = Integer.parseInt(cards.nextToken());
            }

            // 규영이의 패를 정렬 (인영이의 패가 될 수 있는 숫자들을 편하게 결정하기 위해)
            Arrays.sort(gNums);
            // 인영이의 패를 만듦
            makeINums();

            // 인영이의 패가 될 수 있는 모든 경우를 찾아서 승패 여부를 계산
            permutation(0);
            // 승패 결과를 result 변수에 대입
            result.append("#").append(i).append(" ").append(win).append(" ").append(lose).append("\n");
        }

        // 결과를 출력
        System.out.println(result);
    }
}
/**
 * 아이디어 : 학생을 입력받을 때 마다 규칙을 고려하여 학생들을 배정
 * 시간 : 160 ms
 * 메모리 : 17336 kb
 * 난이도 : 상 (구현이 너무 복잡해서 아이디어를 생각하고 코드를 짜기가 힘들었던거 같습니다.)
 */

package Algorithm;

import java.io.*;
import java.util.*;

public class Main_21608_상어_초등학교 {
    static int N;                                               // 교실의 가로 세로 칸수
    static int[][] board;                                       // 학생들의 자리를 저장할 배열
    static int[] dx = {0, 0, -1, 1};                            // 상하좌우 탐색을 위한 변수
    static int[] dy = {-1, 1, 0, 0};                            // 상하좌우 탐색을 위한 변수
    static Map<Integer, int[]> prefMap = new HashMap<>();       // 각 학생의 선호 학생들을 저장할 변수

    // 주어진 좌표가 경계안에 속하는지 체크하는 함수
    static boolean boundCheck(int y, int x) {
        return x >= 0 && x < N && y >= 0 && y < N;
    }

    // 주어진 값이 배열안에 존재하는지 확인하는 함수
    static boolean isIn(int val, int [] array) {
        boolean check = false;  // 배열안에 존재하는지 체크하기 위한 변수

        // 반복문을 돌며 주어진 값이 배열에 존재하면 check 변수에 true를 대입하고 break
        for (int j : array) {
            if (val == j) {
                check = true;
                break;
            }
        }

        return check;   // check 변수 반환
    }

    // 상하좌우 이동을 하며 인접한 칸에 좋아하는 학생이 몇명 있는지 카운트하는 함수
    static int countLikes(int y, int x, int [] likes) {
        int count = 0;  // 좋아하는 학생 수

        // 상하좌우 이동을 하며 인접한 칸에 좋아하는 학생의 수를 셈
        for (int i = 0; i < 4; i++) {
            int newY = y + dy[i];   // 새로운 y 좌표
            int newX = x + dx[i];   // 새로운 x 좌표

            // 이동한 새로운 좌표가 경계안에 있고 좋아하는 해당 자리 학생이 좋아하는 학생 리스트에 있으면
            if (boundCheck(newY, newX) && isIn(board[newY][newX], likes)) {
                count++;    // 카운트 증가
            }
        }

        return count;   // 카운트 반환
    }

    // 빈칸의 개수를 세는 함수
    static int countEmpty(int y, int x) {
        int count = 0;  // 빈칸의 개수

        // 상하좌우 이동을 하며 빈칸의 개수를 셈
        for (int i = 0; i < 4; i++) {
            int newY = y + dy[i];   // 새로운 y 좌표
            int newX = x + dx[i];   // 새로운 x 좌표

            // 이동한 새로운 좌표가 경계안에 있고 빈칸이면
            if (boundCheck(newY, newX) && board[newY][newX] == 0) {
                count++;    // 카운트 증가
            }
        }

        return count;   // 카운트 반환
    }

    // 만족도 점수를 계산할 함수
    static int satisfactionScore(int y, int x, int[] likes) {
        int count = 0;  // 좋아하는 학생 수

        // 상하좌우 이동을 하며 인접한 칸에 좋아하는 학생의 수를 셈
        for (int i = 0; i < 4; i++) {
            int newY = y + dy[i];     // 새로운 y 좌표
            int newX = x + dx[i];     // 새로운 x 좌표

            // 이동한 새로운 좌표가 경계안에 있고 해당 자리 학생이 좋아하는 학생 리스트 안에 있으면
            if (boundCheck(newY, newX) && isIn(board[newY][newX], likes)) {
                count++;    // 카운트 증가
            }
        }

        // 좋아하는 학생 수에 따라 점수를 반환
        switch (count) {
            case 1: return 1;       // 1명이면 1점 반환
            case 2: return 10;      // 2명이면 10점 반환
            case 3: return 100;     // 3명이면 100점 반환
            case 4: return 1000;    // 4명이면 1000점 반환
            default: return 0;      // 0명이면 0점 반환
        }
    }

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        N = Integer.parseInt(br.readLine());    // 교실의 가로 세로 칸수
        board = new int[N][N];                  // 교실 배열

        // N^2 개의 줄에 학생 번호와 좋아하는 학생의 번호를 입력받고 자리 배정
        for (int i = 0; i < N * N; i++) {
            StringTokenizer st = new StringTokenizer(br.readLine());

            int number = Integer.parseInt(st.nextToken());  // 학생 번호를 입력받음
            int [] likes = new int[4];

            // 좋아하는 학생의 번호를 입력받음
            for (int j = 0; j < 4; j++) {
                likes[j] = Integer.parseInt(st.nextToken());
            }

            prefMap.put(number, likes); // 학생의 번호를 키로 좋아하는 학생의 번호를 값으로 맵에 추가

            int maxLikeCount = -1;      // 최대 좋아하는 학생수
            int maxEmptyCount = -1;     // 최대 빈칸 수
            int x = -1;                 // 학생을 배정할 x 좌표
            int y = -1;                 // 학생을 배정할 y 좌표

            // 매 칸 마다 기준에 따라 자리 배정
            for (int j = 0; j < N; j++) {
                for (int k = 0; k < N; k++) {
                    // 이미 학생이 배정된 칸이면 continue
                    if (board[j][k] != 0)
                        continue;

                    // 인접한 칸의 좋아하는 학생 수를 카운트하여 대입
                    int likeCount = countLikes(j, k, likes);
                    // 인접한 칸의 비어있는 칸수를 카운트하여 대입
                    int emptyCount = countEmpty(j, k);

                    // 만약 현재 최대 좋아하는 학생 수 보다 이번 좋아하는 학생수가 더 많으면
                    if (maxLikeCount < likeCount) {
                        maxLikeCount = likeCount;       // maxLikeCount 값 업데이트
                        maxEmptyCount = emptyCount;     // maxEmptyCount 값 업데이트

                        // 현재 좌표를 학생 배정 좌표로 결정
                        y = j;
                        x = k;
                    }
                    // 만약 인접한 칸에 좋아하는 학생 수가 같은 여러 칸이 존재할 경우 비어있는 칸이 많은 곳에 배치
                    else if ((maxLikeCount == likeCount) && (maxEmptyCount < emptyCount)) {
                        maxEmptyCount = emptyCount; // maxEmptyCount 값 업데이트

                        // 현재 좌표를 학생 배정 좌표로 결정
                        y = j;
                        x = k;
                    }
                    // 만약 좋아하는 학생 수와 비어있는 칸수 까지 같은 칸이 여러개이면 행의 번호가 더 작은 칸으로 배치
                    else if ((maxLikeCount == likeCount) && (maxEmptyCount == emptyCount) && (y > j)) {
                        // 현재 좌표를 학생 배정 좌표로 결정
                        y = j;
                        x = k;
                    }
                    // 만약 좋아하는 학생 수와 비어있는 칸수, 행의 번호가 같은 칸이 여러개이면 열의 번호가 작은 칸으로 배치
                    else if ((maxLikeCount == likeCount) && (maxEmptyCount == emptyCount) && (y == j) && (x > k)) {
                        // 현재 좌표를 학생 배정 좌표로 결정
                        y = j;
                        x = k;
                    }
                }
            }

            board[y][x] = number;   // 구한 좌표에 학생 배치
        }

        // 정해진 자리에 따라 학생 선호도 계산
        int totalLikes = 0;     // 학생 선호도 저장을 위한 변수

        // 반복문을 돌며 각 칸마다 학생 선호도 점수를 계산
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
                int student = board[i][j];  // 현재 위치의 학생 번호를 구함

                int [] likes = prefMap.get(student);    // 학생 번호로 선호하는 학생들을 구함
                totalLikes += satisfactionScore(i, j, likes);   // 이를 통해 학생 선호도 점수를 구해서 전체 학생 선호도 점수에 합함
            }
        }

        System.out.println(totalLikes);     // 결과 출력
    }
}
/**
 * 아이디어 : 1~N까지의 모든 수열을 구하고 칼로리의 총합이 제한선을 넘지 않는 애들만 따로 저장, 그 후 맛 선호도 최디 갮을 찾음
 * 시간 : 1,090 ms
 * 메모리 : 115,776 kb
 * 난이도 : 중 (수열 개념에 칼로리 체크와 선호도 최대값 도출 로직만 추가하면되서 무난하게 풀 수 있었습니다.)
 */

import java.io.*;
import java.util.*;

// 재료의 정보를 저장할 클래스 선언
class Ingredient {
    int flavor;     // 맛 선호도 점수
    int calorie;    // 칼로리

    // 생성자
    public Ingredient(int flavor, int calorie) {
        this.flavor = flavor;
        this.calorie = calorie;
    }
}

public class Solution {
    static int N, L;                        // 재료의 개수와 최대 칼로리
    static Ingredient[] ingredients;        // 재료를 저장할 배열
    static Ingredient[] selections;         // 선택한 재료를 저장할 배열
    static ArrayList<Ingredient[]> result;  // 수열을 통해 찾은 모든 경우의 수를 저장할 리스트

    // 수열 조합 세트 중 맛에 대한 선호도 합이 가장 높은 세트의 선호도 합을 반환하는 함수
    static int getMaxFlavor() {
        int maxFlavor = 0;  // 최대 선호도 합

        // 반복문을 돌며 수열 케이스를 하나씩 뽑음
        for (Ingredient[] selectedIngredients: result) {
            int sumFlavor = 0;  // 수열 케이스에서 맛 점수의 합

            // 수열 케이스를 하나씩 돌며 재료를 하나씩 꺼내 맛 점수의 합을 구함
            for (Ingredient selectedIngredient: selectedIngredients) {
                sumFlavor += selectedIngredient.flavor;
            }

            // 최대 선호도 합 보다 현재 선호도 합이 더 크면 대입
            if (maxFlavor < sumFlavor)
                maxFlavor = sumFlavor;
        }

        return maxFlavor;   // 최대 맛 점수를 반환
    }

    static void combination(int cnt, int start, int R) {
        // 햄버거 재료 수열을 완성했으면
        if (cnt == R) {
            int sum = 0;    // 칼로리의 총합을 저장할 변수

            // 칼로리의 총합을 구한 후
            for (Ingredient i: selections) {
                sum += i.calorie;
            }

            // 제한된 칼로리 이하면
            if (sum <= L)
                // 모든 경우의 수를 저장할 리스트에 추가
                result.add(Arrays.copyOf(selections, selections.length));

            return; // 함수 종료
        }

        // 첫번째 부터 마지막 재료까지 돌면서 탐색 진행
        for (int i = start; i < N; i++) {
            selections[cnt] = ingredients[i];       // 선택한 재료에 현재 재료 추가
            combination(cnt+1, i+1, R);    // 다음 재료를 찾으러 감
        }
    }

    public static void main(String[] args) throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int T = Integer.parseInt(br.readLine());    // 테스트 케이스 개수를 입력받음

        // 테스트 케이스 개수만큼 연산 처리
        for (int i = 1; i <= T; i++) {
            StringTokenizer st = new StringTokenizer(br.readLine());
            N = Integer.parseInt(st.nextToken());   // 재료 개수를 입력받음
            L = Integer.parseInt(st.nextToken());   // 최대 칼로리를 입력받음

            ingredients = new Ingredient[N];    // ingredients 배열 초기화

            // 반복문을 돌며 재료를 입력받음
            for (int j = 0; j < N; j++) {
                st = new StringTokenizer(br.readLine());

                int flavor = Integer.parseInt(st.nextToken());  // 맛 점수를 입력받음
                int calorie = Integer.parseInt(st.nextToken()); // 칼로리를 입력받음

                ingredients[j] = new Ingredient(flavor, calorie); // 새로운 재료 추가
            }

            int totalMaxFlavor = 0; // 전체 최대 맛 점수 합

            // 재료를 1개 부터 N개 고르는 모든 수열을 구해서 최대 맛 점수 합을 구함
            for (int j = 1; j <= N; j++) {
                selections = new Ingredient[j];  // selections 배열 초기화
                result = new ArrayList<>();      // result 리스트 초기화
                combination(0, 0, j);   // nCj 수열을 찾으러 감
                int maxFlavor = getMaxFlavor();  // 최대 맛 점수 합을 구함

                // 전체 최대 맛 점수 합 보다 현재 맛 점수 합이 더 높으면 대입
                if (totalMaxFlavor < maxFlavor)
                    totalMaxFlavor = maxFlavor;
            }

            // 결과를 StringBuilder에 저장
            sb.append("#").append(i).append(" ").append(totalMaxFlavor).append("\n");
        }

        // 결과 출력
        System.out.println(sb);
    }
}
/**
 * 아이디어 : 비트마스킹을 이용하여 20 비트 비트 마스킹 변수에 대해 각 연산을 처리
 * 시간 : 1076 ms
 * 메모리 : 313364 kb
 * 난이도 : 중 (비트 마스킹을 잘 몰라서 정답 코드를 봤는데, 막상 코드를 보니 생각보다 단순했던거 같습니다.)
 */

import java.util.*;
import java.io.*;

public class Main {
    public static void main(String[] args) throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int M = Integer.parseInt(br.readLine());    // 명령어의 개수를 입력받음
        int bitMask = 0;                            // 비트 마스킹에 사용할 변수

        // M개의 명령어를 입력받아 처리
        for (int i = 0; i < M; i++) {
            StringTokenizer st = new StringTokenizer(br.readLine());    // 명령어를 입력받음
            String command = st.nextToken();    // 명령 부분을 잘라냄
            int value = 0;

            // 명령어 뒤에 붙은 값이 있으면 값을 잘라냄
            if (st.hasMoreTokens()) {
                value = Integer.parseInt(st.nextToken());
            }

            // switch 문을 돌며 명령 처리
            switch (command) {
                case "add":     // OR 연산을 통해 해당 숫자 자리에 1을 세팅
                    bitMask |= (1 << value);
                    break;
                case "remove":  // 해당 숫자 자리에 0을 AND 연산시켜서 해당 자리 값을 0으로 세팅
                    bitMask &= ~(1 << value);
                    break;
                case "check":   // AND 연산을 통해 해당 자리가 있는지 확인하여 있으면 1 없으면 0을 StringBuilder에 추가
                    sb.append((bitMask & (1 << value)) != 0 ? 1 : 0).append('\n');
                    break;
                case "toggle":  // XOR 연산을 통해 값 반전
                    bitMask ^= (1 << value);
                    break;
                case "all":     // 2^21 - 1을 대입하여 모든 값을 1로 세팅
                    bitMask = (1 << 21) - 1;
                    break;
                case "empty":   // 0을 대입하여 모든 값을 0으로 세팅
                    bitMask = 0;
                    break;
            }
        }

        System.out.println(sb);     // 결과 출력
    }
}
/**
 * 아이디어 : 큐 자료구조를 이용해서 문제의 사이클을 반복합니다. 이후 숫자가 감소해서 0이하가 되면 사이클을 종료하고 암호를 저장합니다.
 * 시간 : 92 ms
 * 메모리 : 26880 kb
 * 난이도 : 하 (문제를 보고 금방 아이디어가 떠올라서 쉽게 구현할 수 있었습니다.)
 */

import java.util.*;
import java.io.*;

public class Solution {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringBuilder sb = new StringBuilder();     // 결과를 저장할 변수

        // 반복문을 돌며 10개의 테스트 케이스를 처리
        for (int i = 0; i < 10; i++) {
            // T를 입력받음
            int T = Integer.parseInt(br.readLine());
            // 암호문 제작에 사용될 8개의 숫자를 입력받음
            StringTokenizer st = new StringTokenizer(br.readLine());

            // 암호문 제작에 사용할 큐를 생성
            ArrayDeque<Integer> que = new ArrayDeque<>();

            // 8개의 암호문에 사용될 수를 큐에 추가
            for (int j = 0;  j < 8; j++) {
                que.offer(Integer.parseInt(st.nextToken()));
            }

            // 사이클을 돌 때 숫자에서 1 ~ 5씩 돌아가면서 빼기 위해 count 변수 선언
            int count = 1;

            // 무한루프를 돌며 암호문 생성
            while(true) {
                int first = que.poll();     // 맨앞 숫자를 빼서 first 변수에 저장

                // 5의 배수가 아니면 %5를 했을 때 1~4 값이 그대로 나오는 것을 활용
                if (count % 5 > 0) {
                    first -= count % 5;
                }
                // 5의 배수이면 %5를 했을 때 0이 되기 때문에 5를 뺌
                else {
                    first -= 5;
                }

                count++;    // count 증가

                // 만약 count를 뺀 숫자가 0이하기 되면
                if (first <= 0) {
                    que.offer(0);   // 0을 암호문 맨뒤에 삽입하고
                    break;             // 암호문 제작을 위한 반복문 종료
                }

                que.offer(first);       // 아니면 그냥 first를 맨 뒤에 삽입
            }

            // 암호문 StringBuilder에 삽입
            sb.append("#").append(T).append(" ");
            while(!que.isEmpty())
                sb.append(que.poll()).append(" ");
            sb.append("\n");
        }

        // 암호문 출력
        System.out.println(sb);
    }
}
/**
 * 아이디어 : 알파벳 개수만큼의 배열을 만들고 알파벳 - 'a' 를 통해 해당 인덱스를 구해 증감연산으로 빈도를 측정
 * 시간 : 120 ms
 * 메모리 : 15892 kb
 * 난이도 : 하
 */

import java.io.*;

public class Main {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String str = br. readLine();    // 숫자들을 입력받음
        int [] alpha = new int [26];    // 숫자 빈도 측정을 위한 배열 생성

        // 반복문을 돌며 빈도 측정
        for (char c: str.toCharArray()) {
            alpha[c-'a']++; // 알파벳 - 'a' 를 하게 되면 인덱스가 나옴
        }

        // 빈도 측정 결과 출력
        for (int n: alpha) {
            System.out.print(n + " ");
        }
    }
}
/**
 * 아이디어 : 6과 9를 타겟으로 최소 세트 개수를 구하고 6과 9를 제외한 숫자를 타겟으로 최소 세트를 구해서 더 큰 값을 사용
 *          6과 9는 서로 대체할 수 있기 때문에 6과 9를 합하고 2를 나눈 값을 올림하여 별도로 계산 진행
 * 시간 : 104 ms
 * 메모리 : 14332 kb
 * 난이도 : 중 (구현 아이디어가 생각보다 잘 떠오르지 않아서 시간이 걸렸습니다.)
 */

package Algorithm;

import java.io.*;
import java.util.*;

public class Main_1475_방번호 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        Map<Integer, Integer> map = new HashMap<>();    // 숫자 빈도를 저장하기 위한 변수

        // 0부터 9까지 초기화
        for (int i = 0; i < 10; i++) {
            map.put(i, 0);
        }

        String number = br.readLine();  // 숫자를 입력받음

        // 각 숫자의 빈도를 카운트
        for (char n : number.toCharArray()) {
            int digit = n - '0';
            map.put(digit, map.get(digit) + 1);
        }

        // 6과 9를 타겟으로 최소 세트를 구함
        // 6과 9를 합산하고 2로 나눈 올림 값 계산
        int sixNine = map.get(6) + map.get(9);
        int combined = (int) Math.ceil(sixNine / 2.0);

        // 6, 9 제외한 다른 숫자들의 최대 빈도 계산
        int max = 0;
        for (int i = 0; i < 10; i++) {
            if (i == 6 || i == 9)
                continue;
            max = Math.max(max, map.get(i));
        }

        max = Math.max(max, combined);  // 둘 중 더 큰 값을 사용
        System.out.println(max);        // 결과 출력
    }
}

/*
 * 아이디어 : 숫자를 문자열로 변환하고 반복문을 돌며 빈도 측정
 * 시간 : 108 ms
 * 메모리 : 14224 kb
 * 난이도 : 하
 */

import java.io.*;

public class Main {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int A = Integer.parseInt(br.readLine());    // A를 입력받음
        int B = Integer.parseInt(br.readLine());    // B를 입력받음
        int C = Integer.parseInt(br.readLine());    // C를 입력받음

        int R = A * B * C;          // A * B * C를 R에 대입
        String sR = R + "";         // R을 문자열로 변환하여 대입
        int [] nums = new int[10];  // 빈도 측정을 위한 배열 선언

        // 숫자빈도 측정
        for (char c: sR.toCharArray()) {
            int n = c - '0';    // 숫자 - '0' 을 하면 해당 인덱스가 나옴
            nums[n]++;
        }

        // 숫자 빈도 출력
        for (int n: nums) {
            System.out.println(n);
        }
    }
}
/**
 * 아이디어 : 반복문을 돌면서 개수를 셈
 * 시간 : 104 ms
 * 메모리 : 14284 kb
 * 난이도 : 하
 */

import java.util.*;
import java.io.*;

public class Main {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int N = Integer.parseInt(br.readLine());    // 숫자의 개수를 입력받음
        int [] numbers = new int[N];                // 입력받은 수를 저장할 배열 초기화

        StringTokenizer st = new StringTokenizer(br.readLine());    // 숫자들을 입력받음
        for (int i = 0; i < N; i++) {
            // 입력받은 숫자들로 배열 초기화
            numbers[i] = Integer.parseInt(st.nextToken());
        }

        int target = Integer.parseInt(br.readLine());   // target 수를 입력받음

        int count = 0;  // target이 입력받은 숫자에 몇개 들어 있는지 셀 변수

        // 입력받은 숫자들에 target이 몇개 있는지 셈
        for (int n: numbers) {
            if (n == target) {
                count++;
            }
        }

        System.out.println(count);  // 결과 출력
    }
}