package Jieun714;
/**
 * 문제: 미키의 뒷마당에는 특정 수의 양이 있다. 그가 푹 잠든 사이에 배고픈 늑대는 마당에 들어와 양을 공격했다. 마당은 행과 열로 이루어진 직사각형 모양이다. 글자 '.' (점)은 빈 필드를 의미하며, 글자 '#'는 울타리를, 'o'는 양, 'v'는 늑대를 의미한다.
 *      한 칸에서 수평, 수직만으로 이동하며 울타리를 지나지 않고 다른 칸으로 이동할 수 있다면, 두 칸은 같은 영역 안에 속해 있다고 한다. 마당에서 "탈출"할 수 있는 칸은 어떤 영역에도 속하지 않는다고 간주한다.
 *      다행히 우리의 양은 늑대에게 싸움을 걸 수 있고 영역 안의 양의 수가 늑대의 수보다 많다면 이기고, 늑대를 우리에서 쫓아낸다. 그렇지 않다면 늑대가 그 지역 안의 모든 양을 먹는다.
 *      맨 처음 모든 양과 늑대는 마당 안 영역에 존재한다. 아침이 도달했을 때 살아남은 양과 늑대의 수를 출력하는 프로그램을 작성하라.
 * 입력: 첫 줄에는 두 정수 R과 C가 주어지며(3 ≤ R, C ≤ 250), 각 수는 마당의 행과 열의 수를 의미한다.
 *      다음 R개의 줄은 C개의 글자를 가진다. 이들은 마당의 구조(울타리, 양, 늑대의 위치)를 의미한다.
 * 출력: 하나의 줄에 아침까지 살아있는 양과 늑대의 수를 의미하는 두 정수를 출력한다.
 * 해결: bfs.
 * */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.StringTokenizer;

public class BOJ3184 {
    public static int R, C;
    public static char [][] yard;
    public static boolean [][] isVisited;
    public static int [] count;

    // 방문 가능한 위치 체크
    public static boolean isCheck(int y, int x){
        if(0<=y && y < R && 0<= x && x<C){
            if(!isVisited[y][x] && yard[y][x] != '#')
                return true;
        }
        return false;
    }

    public static void bfs(int y, int x){
        int [] dx = {0, 0, -1, 1};
        int [] dy = {1, -1, 0, 0};
        Queue<int []> que = new ArrayDeque<>();
        que.add(new int[] {y, x});
        isVisited[y][x] = true;

        while(!que.isEmpty()){
            int [] now = que.poll();
            for(int i=0; i<4; i++){
                int nx = now[1] + dx[i];
                int ny = now[0] + dy[i];
                //방문 가능하다면
                if(isCheck(ny, nx)){
                    que.add(new int[] {ny, nx}); //이동 가능한 좌표 큐에 삽입
                    isVisited[ny][nx] = true; //방문처리
                    // o'는 양, 'v'는 늑대
                    if(yard[ny][nx] == 'o') count[0]++;
                    else if(yard[ny][nx] == 'v') count[1]++;
                }
            }
        } //while end
    }

    public static void main(String[] args) throws IOException {
        BufferedReader br =  new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        R = Integer.parseInt(st.nextToken()); //마당의 행
        C = Integer.parseInt(st.nextToken()); //마당의 열
        yard = new char[R][C];

        for(int i=0; i<R; i++){
            yard[i] = br.readLine().toCharArray();
        }

        isVisited = new boolean[R][C]; //방문체크할 배열
        int [] alive = new int[2]; //아침까지 살아있는 양과 늑대의 수를 담을 배열. 0: 양, 1: 늑대
        for(int i=0; i<R; i++){
            for(int j=0; j<C; j++){
                if(!isVisited[i][j]){ //아직 방문한 적이 없는 곳이면
                    count = new int[2]; //양과 늑대의 수를 체크할 배열. 0: 양, 1: 늑대
                    bfs(i, j); //bfs 수행
                    if(count[0]>count[1]){ //만약 양의 수가 많다면
                        alive[0] += count[0]; //살아 남은 양의 수 누적
                    } else { //늑대의 수가 양보다 많거나 적다면
                        alive[1] += count[1]; //남아 있는 늑대의 수 누적
                    }
                } //if end
            } //for end
        } //for end
        System.out.printf(alive[0] + " " + alive[1]); //결과 출력
    }
}