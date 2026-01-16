package BaekJoon.String;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
/*
   5534 간판
   [1] baraxbara 의 경우 bar 를 만들 수 있는 방법이 2가지임을 고려하지 못하는 문제
 */
public class Solution5 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());
        String name = br.readLine();
        String[] pre = new String[n];
        for(int i = 0; i < n; i++){
            pre[i] = br.readLine();
        }

        int cnt = 0;

        for(int i = 0; i < n; i++){
            if(name.equals(pre[i])){
                cnt++;
                continue;
            }
            if(name.length() > pre[i].length()){
                continue;
            }
            int[] gap = new int[name.length()];
            for(int k = 0; k < name.length(); k++) {
                for (int j = 0; j < pre[i].length(); j++) {
                    if (name.charAt(k) == pre[i].charAt(j)){
                        gap[k] = j;
                        break;
                    }
                }
            }

            if(gap[gap.length-1] == 0){
                continue;
            }
            int g = gap[1] - gap[0];
            cnt++;
            for(int k = 2; k < gap.length; k++){
                if(gap[k] - gap[k-1] != g){
                    cnt--;
                    break;
                }
            }
        }
        System.out.println(cnt);
    }
}