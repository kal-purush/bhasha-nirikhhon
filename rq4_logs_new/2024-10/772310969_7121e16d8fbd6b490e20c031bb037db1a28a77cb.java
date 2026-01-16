import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/*
* 퀸은 상하좌우 대각선 원하는 만큼 이동 가능한 이동 규칙을 가졌다.
* 퀸을 서로 잡을 수 없는 위치에 놓아야 한다.
* => 퀸의 이동 경로에 겹치치 않도록 해야 한다.
*
*/


public class B9663 {

    public static int n;
    public static int [] arr;
    public static int cnt = 0;  // 서로 공격할 수 없게 놓는 경우의 수


    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        n = Integer.parseInt(br.readLine());
        arr = new int[n];

        nQueen(0);
        System.out.println(cnt);

    }


    public  static void nQueen(int depth) {
        //행을 다 채우면 카운트 1 증가, 리턴
        if(depth == n){
            cnt++;
            return;
        }

        for(int i = 0; i < n; i++){
            arr[depth] = i;
            if(Possiblity(depth)) {
                nQueen(depth + 1);
            }
        }
    }

    public static boolean Possiblity(int col) {
        for(int i = 0; i < col; i++){
            //같은 행에 존재할 경우 (열의 행과 i열의 행이 일치하는 경우)
            if(arr[col] == arr[i]){
                return false;
            }
            //대각선 (열의 차와 행의 차가 같다면 대각선임)
            else if (Math.abs(col-i)== Math.abs(arr[col]-arr[i])){
                return false;
            }
        }
        return true;
    }
}