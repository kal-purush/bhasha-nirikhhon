package JiEung2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class BOJ2231 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());
        int tmp = 0;
        int answer = 0;
        while(true){
            if(tmp>=n){
                answer = 0;
                break;
            }
            int sum = 0;
            sum = tmp + Solution(tmp);
            if(sum == n){
                answer = tmp;
                break;
            }
            tmp++;
        }
        System.out.println(answer);
    }

    public static int Solution(int n){
        int answer = 0;
        while(n>0){
            answer += n % 10;
            n/=10;
        }
        return answer;
    }

}