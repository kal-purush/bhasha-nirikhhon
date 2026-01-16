package PHS00;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

// 숫자 카드
public class BOJ_10815 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        StringTokenizer st = null;

        int N = Integer.parseInt(br.readLine());
        Set<Integer> cards = new HashSet<>();
        st = new StringTokenizer(br.readLine());
        for(int i = 0; i < N; i++){
            cards.add(Integer.parseInt(st.nextToken()));
        }
        int M = Integer.parseInt(br.readLine());
        st = new StringTokenizer(br.readLine());
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < M; i++){
            if(cards.contains(Integer.parseInt(st.nextToken()))){
                sb.append(1 + " ");
            }else {
                sb.append(0 + " ");
            }
        }
        System.out.println(sb.toString());
    }
}