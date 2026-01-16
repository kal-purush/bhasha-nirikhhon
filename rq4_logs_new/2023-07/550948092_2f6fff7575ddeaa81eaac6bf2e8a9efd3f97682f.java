package DoIt.SlidingWindow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

/*
    009 DNA 비밀번호
 */
public class Solution2 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        int n = Integer.parseInt(st.nextToken());
        int m = Integer.parseInt(st.nextToken());
        String s = br.readLine();
        char[] ch = s.toCharArray();
        int result = 0;
        int[] num = new int[4];
        st = new StringTokenizer(br.readLine());
        for(int i = 0; i < 4; i++){
            num[i] = Integer.parseInt(st.nextToken());
        }
        int[] cnt = new int[4];
        for(int i = 0; i < m; i++){
            if(ch[i] == 'A'){
                cnt[0]++;
            } else if(ch[i] == 'C'){
                cnt[1]++;
            } else if(ch[i] == 'G'){
                cnt[2]++;
            } else if(ch[i] == 'T'){
                cnt[3]++;
            }
        }
        boolean possible = true;
        for(int i = 0; i < 4; i++){
            if(cnt[i] < num[i])
                possible = false;
        }
        if(possible == true)
            result++;
        int i = 0, j = m;
        while (j < n){
            possible = true;
            if(ch[i] == 'A'){
                cnt[0]--;
            } else if(ch[i] == 'C'){
                cnt[1]--;
            } else if(ch[i] == 'G'){
                cnt[2]--;
            } else if(ch[i] == 'T'){
                cnt[3]--;
            }

            if(ch[j] == 'A'){
                cnt[0]++;
            } else if(ch[j] == 'C'){
                cnt[1]++;
            } else if(ch[j] == 'G'){
                cnt[2]++;
            } else if(ch[j] == 'T'){
                cnt[3]++;
            }

            for(int k = 0; k < 4; k++){
                if(cnt[k] < num[k])
                    possible = false;
            }
            if(possible == true)
                result++;
            i++; j++;
        }
        System.out.println(result);
    }
}