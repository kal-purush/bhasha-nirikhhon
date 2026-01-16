import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

public class bk_17503 {
    static BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    static StringTokenizer st;
    static StringBuilder sb = new StringBuilder();
    static int n, m, k;
    static Beer[] beers; // 맥주 정보를 담는 배열

    public static void main(String[] args) throws IOException {
        st = new StringTokenizer(br.readLine());
        n = Integer.parseInt(st.nextToken()); // 축제기간
        m = Integer.parseInt(st.nextToken()); // 채울 선호도의 합(목표값)
        k = Integer.parseInt(st.nextToken()); // 맥주 종류의 개수

        beers = new Beer[k];

        for (int i = 0; i < k; i++) {
            st = new StringTokenizer(br.readLine());
            int preference = Integer.parseInt(st.nextToken());
            int alcohol = Integer.parseInt(st.nextToken());
            beers[i] = new Beer(preference, alcohol);
        }

        // 도수 레벨을 기준으로 맥주를 정렬
        Arrays.sort(beers, (b1, b2) -> b1.alcohol - b2.alcohol);

        binarySearch();
        System.out.println(sb);
    }

    private static void binarySearch() {
        int left = 0; // 최소 도수 레벨 (0)
        int right = k - 1; // 최대 도수 레벨의 인덱스
        int target = -1;

        while (left <= right) {
            int mid = (left + right) / 2;
            if (maxDrink(beers[mid].alcohol)) {
                target = beers[mid].alcohol;
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        sb.append(target);
    }

    private static boolean maxDrink(int maxAlcoholLevel) {
        PriorityQueue<Integer> pq = new PriorityQueue<>();
        int preferenceTotal = 0;

        for (Beer beer : beers) {
            if (beer.alcohol > maxAlcoholLevel) break;
            pq.add(beer.preference);
            preferenceTotal += beer.preference;
            if (pq.size() > n) {
                preferenceTotal -= pq.poll();
            }
        }

        return pq.size() == n && preferenceTotal >= m;
    }

    static class Beer {
        int preference;
        int alcohol;

        Beer(int preference, int alcohol) {
            this.preference = preference;
            this.alcohol = alcohol;
        }
    }
}