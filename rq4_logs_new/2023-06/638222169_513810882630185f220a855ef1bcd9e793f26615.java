package BaekJoon.no25330_SHOWMETHEDUNGEON;
package BaekJoon.no2533_사회망서비스SNS;
package BaekJoon.no2548_대표자연수;
package BaekJoon.no2559_수열;
package BaekJoon.no2563_색종이;
package BaekJoon.no2564_경비원;
package BaekJoon.no2565_전깃줄;
package BaekJoon.no2567_색종이2;
package BaekJoon.no2568_전깃줄2;
package BaekJoon.no25707_팔찌만들기;
package BaekJoon.no2573_빙산;
package BaekJoon.no25757_임스와함께하는미니게임;
package BaekJoon.no2578_빙고;
package BaekJoon.no2579_계단오르기;
package BaekJoon.no25826_2차원배열다중업데이트단일합_unsolved;
package BaekJoon.no2583_영역구하기;
package BaekJoon.no2594_놀이공원;
package BaekJoon.no2596_비밀편지;
package BaekJoon.no2597_줄자접기;
package BaekJoon.no2597_줄자접기;
package BaekJoon.no2605_줄세우기;
package BaekJoon.no2615_오목;
package BaekJoon.no26215_눈치우기;
package BaekJoon.no26264_빅데이터정보보호;
package BaekJoon.no2628_종이자르기;
package BaekJoon.no2628_종이자르기;
package BaekJoon.no2629_양팔저울;
package BaekJoon.no2635_수이어가기;
package BaekJoon.no2636_치즈;
package BaekJoon.no2641_다각형그리기;
package BaekJoon.no2659_십자카드문제;
package BaekJoon.no2665_미로만들기;
package BaekJoon.no2667_단지번호붙이기;
package BaekJoon.no2668_숫자고르기;
package BaekJoon.no2669_직사각형네개의합집합의면적구하기;
package BaekJoon.no2671_잠수함식별;
package BaekJoon.no2751_수정렬하기2;
package BaekJoon.no2751_수정렬하기2;
package BaekJoon.no2805_나무자르기;
package BaekJoon.no2812_크게만들기;
package BaekJoon.no2839_설탕배달;
package BaekJoon.no2841_외계인의기타연주;
package BaekJoon.no2847_게임을만든동준이;
package BaekJoon.no2869_달팽이는올라가고싶다;
package BaekJoon.no2879_코딩은예쁘게;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class Main {
    static int[][] tabs;

    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st;

        int n = Integer.parseInt(br.readLine());
        tabs = new int[2][n];
        for (int i = 0; i < 2; ++i) {
            st = new StringTokenizer(br.readLine());
            for (int j = 0; j < n; ++j) tabs[i][j] = Integer.parseInt(st.nextToken());
        }

        int ans = 0;
        int flag = 0;
        int op = 0;
        for (int i = 0; i < n; ++i) {
            int temp = tabs[1][i] - tabs[0][i];
            if (flag == 0) {
                flag = temp;
                op = i;
            }

            if (temp == 0 || (temp > 0 && flag < 0) || (temp < 0 && flag > 0)) {
                ans += solve(op, i - 1);
                flag = temp;
                op = i;
            }
        }
        ans += solve(op, n - 1);
        System.out.print(ans);
    }

    private static int solve(int op, int ed) {
        if (op > ed) return 0;
        else if (op == ed) return Math.abs(tabs[1][op] - tabs[0][op]);

        int cnt = 0;
        int idx = op;
        int limit = Integer.MAX_VALUE;
        boolean flag = tabs[1][op] - tabs[0][op] > 0 ? true : false;

        for (int i = op; i <= ed; ++i) {
            int temp = Math.abs(tabs[1][i] - tabs[0][i]);
            if (temp < limit) {
                idx = i;
                limit = temp;
            }
        }

        for (int i = op; i <= ed; ++i) {
            if (flag) tabs[0][i] += limit;
            else tabs[0][i] -= limit;
        }
        cnt += limit;
        return cnt + solve(op, idx - 1) + solve(idx + 1, ed);
    }
}
package BaekJoon.no2929_머신코드;
package BaekJoon.no2961_도영이가만든맛있는음식;
package BaekJoon.no2986_파스칼;
package BaekJoon.no2999_비밀이메일;
package BaekJoon.no3034_앵그리창영;
package BaekJoon.no3040_백설공주와일곱난쟁이;
package BaekJoon.no3077_임진왜란;
package BaekJoon.no3109_빵집;
package BaekJoon.no3187_양치기꿍;
package BaekJoon.no3197_백조의호수;
package BaekJoon.no3518_공백왕빈칸;
package BaekJoon.no3584_가장가까운공통조상;
package BaekJoon.no3985_롤케이크;
package BaekJoon.no4108_지뢰찾기;
package BaekJoon.no4358_생태학;
package BaekJoon.no4485_녹색옷입은애가젤다지;
package BaekJoon.no4948_베르트랑공준;
package BaekJoon.no4963_섬의개수;
package BaekJoon.no5014_스타트링크;
package BaekJoon.no5397_키로거;