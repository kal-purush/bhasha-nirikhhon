package Beakjoon;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class Beak_1051 {

    static int n, m;
    static int[][] map;

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());

        n = Integer.parseInt(st.nextToken());
        m = Integer.parseInt(st.nextToken());

        map = new int[n][m];
        int answer = 1;
        int size;

        for (int i = 0; i < n; i++) {
            String[] line = br.readLine().split("");
            for (int j = 0; j < m; j++) {
                map[i][j] = Integer.parseInt(line[j]);
            }
        }

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                for (int k = 0; k < Math.min(n, m); k++) {
                    if (i + k < n && j + k < m) {
                        if (map[i][j] == map[i][j + k] && map[i][j] == map[i + k][j] && map[i][j] == map[i + k][j + k]) {
                            size = (k + 1) * (k + 1);
                            answer = Math.max(answer, size);
                        }
                    }
                }
            }
        }

        System.out.println(answer);
    }
}