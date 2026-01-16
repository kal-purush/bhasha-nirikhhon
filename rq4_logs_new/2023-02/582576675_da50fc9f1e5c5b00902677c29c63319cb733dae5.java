import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
/*
https://www.acmicpc.net/problem/1759
메모리 : 14480 KB,	시간 : 128 ms

다른 dfs 문제와 비슷하지만 자음모음의 개수를 체크해야 하는 문제.
*/
public class BOJ_1759 {

    static int L, C, cnt_c;
    static String[] arr, result;
    static boolean[] visited;
    static StringBuilder sb;
    static List<String> cons = new ArrayList<>();

    public static void main(String[] args) throws Exception{
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        L = Integer.parseInt(st.nextToken());
        C = Integer.parseInt(st.nextToken());

        arr = br.readLine().split(" ");
        Arrays.sort(arr);

        result = new String[L];
        visited = new boolean[C];

        sb = new StringBuilder();
        cons.add("a");
        cons.add("e");
        cons.add("i");
        cons.add("o");
        cons.add("u");
        dfs(0,0);
        System.out.print(sb);
    }

    public static void dfs(int count, int start){
        if (count == L){
            for (String s : result) {
                if(cons.contains(s)) cnt_c++;
            }
            if (1 <= cnt_c && cnt_c <= L-2){
                for (String s : result) {
                    sb.append(s);
                }
                sb.append("\n");
            }
            cnt_c = 0;
            return;
        }
        for (int i = start; i < C; i++) {
            if (!visited[i]){
                visited[i] = true;
                result[count] = arr[i];
                dfs(count+1, i+1);
                visited[i] = false;
            }
        }
    }
}