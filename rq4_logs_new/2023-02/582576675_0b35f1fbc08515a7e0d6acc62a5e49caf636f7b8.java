import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.StringTokenizer;

/**
 * BOJ 6603: 로또
 * - 메모리: 15164 kb
 * - 시간: 136 ms
 *
 * [풀이]
 * - 주어진 숫자 K개에 대해 kC6을 구하는 문제
 * - 조합을 구하면 된다!
 */

public class BOJ_6603 {

    static BufferedReader br;
    static BufferedWriter bw;
    static int[] ans = new int[6];

    public static void main(String[] args) throws Exception {
        br = new BufferedReader(new InputStreamReader(System.in));
        bw = new BufferedWriter(new OutputStreamWriter(System.out));
        StringTokenizer st;

        while (true) {
            st = new StringTokenizer(br.readLine(), " ");
            int k = Integer.parseInt(st.nextToken());

            if (k == 0) break;

            int[] nums = new int[k];
            for (int n = 0; n < k; ++n) {
                nums[n] = Integer.parseInt(st.nextToken());
            }

            getLotto(0, 0, nums);
            bw.write("\n");
        }

        bw.flush();
        bw.close();
        br.close();
    }

    static void getLotto(int start, int depth, int[] nums) throws Exception {
        if (depth == 6) {
            for (int i = 0; i < 6; ++i) {
                bw.write(ans[i] + " ");
            }
            bw.write("\n");
            return;
        }

        for (int i = start, end = nums.length; i < end; ++i) {
            ans[depth] = nums[i];
            getLotto(i + 1, depth + 1, nums);
        }
    }
}

