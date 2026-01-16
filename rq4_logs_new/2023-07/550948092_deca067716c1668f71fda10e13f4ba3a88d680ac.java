package BaekJoon.String;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
/**
 * 2607 비슷한 단어
 * 문제를 항상 꼼꼼하게 읽기!
 */
public class Solution4 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());
        String[] str = new String[n];
        for(int i = 0; i < n; i++){
            str[i] = br.readLine();
        }

        int result = 0;
        String s = str[0];
        for(int i = 1; i < n; i++){
            int cnt = 0;
            int[] word = new int[26];
            for(int j = 0; j < s.length(); j++){
                word[s.charAt(j) - 'A']++;
            }

            for(int j = 0; j < str[i].length(); j++){
                if(word[str[i].charAt(j)- 'A'] > 0){
                    cnt++;
                    word[str[i].charAt(j) - 'A']--;
                }
            }

            if(s.length() == str[i].length() && (s.length() == cnt || s.length() - 1 == cnt)){
                result++;
            } else if(s.length() == str[i].length() - 1 && str[i].length() -1 == cnt){
                result++;
            } else if(s.length() == str[i].length() + 1 && str[i].length() == cnt) {
                result++;
            }
        }
        System.out.println(result);
    }
}