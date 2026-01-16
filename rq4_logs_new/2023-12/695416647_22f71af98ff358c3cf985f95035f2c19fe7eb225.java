package JiEung2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class BOJ6603 {
    static int n;
    static int[] arr;
    static int[] check;
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while(true){
            StringTokenizer st = new StringTokenizer(br.readLine());
            n = Integer.parseInt(st.nextToken());
            if(n == 0) break;
            arr = new int[n];
            check = new int[n];
            for(int i=0; i<n; i++){
                arr[i] = Integer.parseInt(st.nextToken());
            }
            for(int i=0; i<n-5; i++){
                check[i] = 1;
                DFS(0, i);
                check[i] = 0;
            }
            System.out.println();
        }
    }

    public static void DFS(int L, int index){
        if(L == 5){
            for(int i=0; i<n; i++){
                if(check[i] == 1){
                    System.out.print(arr[i] + " ");
                }
            }
            System.out.println();
            return;
        }

        for(int i=index+1; i<n; i++){
            if(check[i] == 0){
                check[i] = 1;
                DFS(L + 1, i);
                check[i] = 0;
            }
        }
    }
}