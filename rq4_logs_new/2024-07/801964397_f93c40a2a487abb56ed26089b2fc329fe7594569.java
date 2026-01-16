import java.io.*;
import java.util.*;


public class code24041 {
	static long n,g, k;
	static long left = 1, right = Integer.MAX_VALUE -1;
	static Ingredient[] ings;

	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		StringTokenizer st = new StringTokenizer(br.readLine());
		n = Long.parseLong(st.nextToken());
		g = Long.parseLong(st.nextToken());
		k = Long.parseLong(st.nextToken());

		ings = new Ingredient[(int) n];
		for (int i = 0; i < n; i++) {
			st = new StringTokenizer(br.readLine());
			ings[i] = new Ingredient(Long.parseLong(st.nextToken()), Long.parseLong(st.nextToken()), Integer.parseInt(st.nextToken()));
		}

		while (left < right) {
			int x = (int)((left + right + 1) / 2);
			boolean isUnderG = chkVirus(x);

			if(isUnderG) left = x;
			else right = x-1;
		}
		System.out.println(left);
	}

	static boolean chkVirus(int x){
		PriorityQueue<Long> pq = new PriorityQueue<>(Collections.reverseOrder());
		long cnt = k;
		long sum = 0;	// 세균수 합
		for (Ingredient ing : ings) {
			// x일 후의 세균 수
			long tmp = ing.s * Math.max(1, x - ing.l);
			if (ing.o) {
				pq.add(tmp);
			}
			sum += tmp;
			if (sum > g) {
				// 안심하고 먹을 수 없는 지경
				if (cnt == 0 || pq.isEmpty()) {
					return false;
				}
				cnt--;
				sum -= pq.poll();
			}
		}
		while(cnt > 0 && !pq.isEmpty()){
			sum -= pq.poll();
			cnt--;
		}
		return sum <= g;
	}

}

class Ingredient {
	long s, l;
	boolean o;

	public Ingredient(long s, long l, int o) {
		this.s = s;
		this.l = l;
		if (o == 1) {
			this.o = true;
		} else {
			this.o = false;
		}
	}
}