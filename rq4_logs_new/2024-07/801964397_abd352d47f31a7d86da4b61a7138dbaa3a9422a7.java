import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.*;

class Main {
    static long N, M, K;
    static List<long[]> list;
    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));

        StringTokenizer st = new StringTokenizer(br.readLine());
        N = Long.parseLong(st.nextToken());
        M = Long.parseLong(st.nextToken());
        K = Long.parseLong(st.nextToken());

        long maxB = -1;

        list = new ArrayList<>();

        for(int i=0; i<K; ++i) {
            st = new StringTokenizer(br.readLine());
            long a = Long.parseLong(st.nextToken());
            long b = Long.parseLong(st.nextToken());
            list.add(new long[] {a, b});

            maxB = Math.max(maxB, b);
        }

        list.sort((v, w) -> Math.toIntExact(w[0] - v[0])); // 선호도로 역순정렬

        long s = 0, e = maxB;
        long ans = -1;
        while(s <= e) {
            long mid = (s + e) / 2;

            if(can(mid)) {
                ans = mid;
                e = mid-1;
            } else {
                s = mid+1;
            }
        }

        System.out.println(ans);
    }

    static boolean can(long target) {
        long cnt = 0;
        long total = 0;
        for(long[] v : list) {
            if(v[1] > target) continue;
            cnt++;
            total += v[0];

            if(cnt == N && total >= M) return true;
        }

        return false;
    }
}