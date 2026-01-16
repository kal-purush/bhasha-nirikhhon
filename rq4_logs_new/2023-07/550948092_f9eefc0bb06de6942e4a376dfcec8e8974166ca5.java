package BaekJoon.String;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;

/**
 * 1755 숫자놀이
 * 정해진(고정된) 값이고, 양이 적다면 치환하는 방법 사용 고민해봐도 좋음
 */
public class Solution3 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());

        String[] num = new String[] {"zero", "one", "two", "three","four","five","six","seven","eight","nine"};

        int m = Integer.parseInt(st.nextToken()), n = Integer.parseInt(st.nextToken());

        String[] str = new String[n-m+1];
        int[] out = new int[n-m+1];
        HashMap<String, Integer> hm = new HashMap<>();

        for(int i = 0; i < 10; i++){
            hm.put(num[i], i);
        }

        int cnt = 0;
        for(int i = m; i <= n; i++){
            if(i < 10){
                str[cnt++] = num[i];
            } else {
                str[cnt++] = num[i/10] + " " + num[i%10];
            }
        }
        Arrays.sort(str);

        for(int i = 0; i < cnt; i++){
            st = new StringTokenizer(str[i]);
            out[i] = hm.get(st.nextToken());
            if(st.hasMoreTokens()){
                out[i] = out[i] * 10 + hm.get(st.nextToken());
            }
        }

        for(int i = 0; i < cnt; i++){
            System.out.print(out[i]+" ");
            if(i % 10 == 9) System.out.println();
        }
    }
}