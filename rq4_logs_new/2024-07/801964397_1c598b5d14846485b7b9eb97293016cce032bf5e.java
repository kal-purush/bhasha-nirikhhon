import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class bk_16173 {
    static BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    static StringTokenizer st;
    static boolean[][] visited;
    static int n;
    static int[] dx = {0, 1};
    static int[] dy = {1, 0};
    static boolean answer = false;

    public static void main(String[] args) throws IOException {
        n = Integer.parseInt(br.readLine());
        int[][] maps = new int[n][n];
        visited = new boolean[n][n];

        for (int i = 0; i < n; i++) {
            st = new StringTokenizer(br.readLine());
            for (int j = 0; j < n; j++) {
                maps[i][j] = Integer.parseInt(st.nextToken());
            }
        }

        dfs(0, 0, maps);

        if (answer) {
            System.out.println("HaruHaru");
        } else {
            System.out.println("Hing");
        }
    }

    private static void dfs(int x, int y, int[][] maps) {
//        종료 조건 : x 또는 y가 map 범위를 벗어나거나, visited가 이미 방문한 위치라면, answer=true(이비 목적지에 도착했다면)
        if (x >= n || y >= n || visited[x][y] || answer) return;
        if (maps[x][y] == -1) {
            answer = true;
            return;
        }
//        방문한 배열 위치, 방문처리
        visited[x][y] = true;

        for (int i = 0; i < 2; i++) {
            int nx = x + dx[i] * maps[x][y];    // maps 좌표에 적힌 위치만큼 움직잃 수 있으므로..
            int ny = y + dy[i] * maps[x][y];
//            bfs 호출
            dfs(nx, ny, maps);
        }
    }
}