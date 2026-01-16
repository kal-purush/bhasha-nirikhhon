package _0217;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.StringTokenizer;

public class Solution {
    static BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
    static StringBuilder output = new StringBuilder();
    static StringTokenizer tokens;

    static int T, N;
    private static int[] weights;

    private static int[][] memo;

    public static void main(String[] args) throws IOException {

        T = Integer.parseInt(input.readLine());
        for (int t = 1; t <= T; t++) {
            N = Integer.parseInt(input.readLine());
            weights = new int[N];
            tokens = new StringTokenizer(input.readLine(), " ");

            int weightSum = 0;
            for (int i = 0; i < N; i++) {
                weightSum += weights[i] = Integer.parseInt(tokens.nextToken());
            }
            memo = new int[weightSum + 1][(1 << N)];

            int answer = solve(N, 0, 0, 0);

            output.append('#').append(t).append(' ').append(answer).append('\n');
        }

        System.out.print(output);
    }

    private static int solve(final int toChoose, final int left, final int right, final int visit) {
        if (left < right) {
            return 0;
        }

        if (memo[left][visit] != 0) {
            return memo[left][visit];
        }

        if (toChoose == 0) {
            return memo[left][visit] = 1;
        }

        int cntSum = 0;

        for (int i = 0; i < N; i++) {
            if ((visit & (1 << i)) == 0) {
                cntSum += solve(toChoose - 1, left + weights[i], right, visit | (1 << i));
                cntSum += solve(toChoose - 1, left, right + weights[i], visit | (1 << i));
            }
        }
        return memo[left][visit] = cntSum;
    }

}
package _0218;

import java.io.BufferedReader;
import java.util.Arrays;
import java.util.Scanner;

public class Main_16919_봄버맨2_문영호 {
    static int R, C, N;
    static boolean[][][] map;

    static StringBuilder sb =new StringBuilder();

    static int[] dx = {-1, 1, 0, 0};
    static int[] dy = {0, 0, -1, 1};

    public static void main(String[] args) {

        Scanner sc = new Scanner(System.in);
        R = sc.nextInt();
        C = sc.nextInt();
        N = sc.nextInt();
        map = new boolean[4][R][C];
        sc.nextLine();
//.......
        for (int i = 0; i < R; i++) {
            String input = sc.nextLine();
            for (int j = 0; j < C; j++) {
                char x = input.charAt(j);
                map[0][i][j] = (x == 'O');
            }
        }

        for(int i = 1; i < 4; i++){
            fillBomb(map[i]);
        }

        fillMap2(map[2]);
        fillMap3();


        if( N % 2 == 0){
            printMap(map[1]);
        }else{
            if (N == 1) {
                printMap(map[0]);
            }else if(N % 4 == 1 ){
                printMap(map[3]);
            }else if(N % 4 == 3){
                printMap(map[2]);
            }

        }

        System.out.println(sb.toString());
    }

    static void fillMap3(){
        for(int i = 0; i < R; i++){
            for(int j = 0; j < C; j++){
                if(map[2][i][j]){
                    map[3][i][j] = false;
                    for(int k = 0; k < 4; k++){
                        int nx = i + dx[k];
                        int ny = j + dy[k];

                        if(isIn(nx, ny)){
                            map[3][nx][ny] = false;
                        }
                    }
                }
            }
        }
    }

    // map 0 에 있던 터트리고
    static void fillMap2(boolean[][] arr){

        for(int i = 0; i < R; i++){
            for(int j = 0; j < C; j++){
                if(map[0][i][j]){
                    arr[i][j] = false;
                    for(int k = 0; k < 4; k++){
                        int nx = i + dx[k];
                        int ny = j + dy[k];

                        if(isIn(nx, ny)){
                            arr[nx][ny] = false;
                        }
                    }
                }
            }
        }
    }

    static boolean isIn(int x, int y){
        return x >= 0 && x < R && y >= 0 && y < C;
    }

    static void fillBomb(boolean[][] map){
        for(boolean[] x : map){
            Arrays.fill(x, true);
        }
    }

    static void printMap(boolean[][] arr) {
        for (int i = 0; i < R; i++) {
            for (int j = 0; j < C; j++) {
                if (arr[i][j])
                    sb.append("O");
                else
                    sb.append(".");
            }

            sb.append("\n");
        }
    }
}