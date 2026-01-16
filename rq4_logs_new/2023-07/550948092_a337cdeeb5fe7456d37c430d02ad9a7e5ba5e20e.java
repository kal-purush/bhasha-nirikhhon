package DoIt.Combination;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.StringTokenizer;

/*
    081 순열의 순서 구하기
    팩토리얼, 경우의 수
    이해가 잘 안되어서 다시 확인할 필요가 있는 문제
 */
public class Solution5 {
    public static void main(String[] args) throws IOException {
        int n, q;
        long F[] = new long[21];
        int S[] = new int[21];
        boolean visited[] = new boolean[21];
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        n = Integer.parseInt(st.nextToken());
        st = new StringTokenizer(br.readLine());
        q = Integer.parseInt(st.nextToken());
        F[0] = 1;
        //팩토리얼 초기화 - 각 자릿수에서 만들 수 있는 경우의 수
        for(int i = 1; i <= n; i++){
            F[i] = F[i-1] * i;
        }
        //소문제 번호 1
        if(q == 1){
            //길이가 n인 순열의 k번 째 순서의 순열 출력
            long k = Long.parseLong(st.nextToken());
            for(int i = 1; i <= n; i++){
                // cnt: 해당 자리에서 몇 번째 수를 이용해야 할지 정하는 변수
                for(int j = 1, cnt = 1; j <= n; j++){
                    if(visited[j])
                        continue;
//                    if(k <= cnt * F[n-i]) {
//                        k -= ((cnt - 1) * F[n-i]); // 현재 순서 -= 해당 자리 순열 수 * (cnt - 1)
//                        S[i] = j;
//                        visited[j] = true;
//                        break;
//                    }
//                    cnt++;
                    //팩토리얼 값이 k보다 작으면 k에서 팩토리얼 값을 빼준다
                    if(F[n - i] < k){
                        k -= F[n- i];
                    } else {
                        //팩토리얼 값이 k보다 크면 해당하는 원소의 숫자 찾은 것
                        S[i] = j;
                        visited[j] = true;
                        break;
                    }
                }
            }
            for(int i = 1; i <= n; i++){
                System.out.println(S[i] + " ");
            }
        } else { //소문제 번호 2
            long k = 1;
            for(int i = 1; i <= n; i++){
                S[i] = Integer.parseInt(st.nextToken());
                long cnt = 0;
                for(int j = 1; j < S[i]; j++){
                    if(visited[j] == false)
                        cnt++; //미사용 개수만큼 증가
                }
                k += cnt * F[n-i];
                visited[S[i]] = true;
            }
            System.out.println(k);
        }
    }
}